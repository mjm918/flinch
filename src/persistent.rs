use std::sync::Arc;
use log::trace;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::{Tree, Iter, IVec, Db};
use crate::document_trait::Document;
use crate::utils::{DOC_PREFIX, get_doc_name};

pub struct Persistent {
    tree: Arc<Tree>
}

impl Persistent {
    pub fn open(db: &Db, name: &str) -> Self {
        let tree = Arc::new(db.open_tree(name).unwrap());
        Self { tree }
    }

    pub fn put<D>(&self, k: String, d: D) where
        D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static + Document {
        self.tree
            .insert(k.to_owned(),IVec::from(d.string().as_str()))
            .expect(format!("inserting {} into local storage",&k).as_str());
    }

    pub fn put_any<D>(&self, k: String, d: D)
        where D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
        self.tree
            .insert(k.to_owned(),IVec::from(serde_json::to_string(&d).unwrap().as_bytes()))
            .expect(format!("inserting TTL {} into local storage",&k).as_str());
    }

    pub fn remove(&self, k: String) -> sled::Result<Option<IVec>> {
        trace!("{} - key removed from storage",&k);
        self.tree.remove(k)
    }

    pub fn get(&self, k: String) -> sled::Result<Option<IVec>> {
        self.tree.get(k)
    }

    pub fn fetch_doc<D>(&self) -> Vec<(String, D)> where
        D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static + Document {
        let documents = self.prefix(format!("{}",DOC_PREFIX));
        let mut res: Vec<(String, D)> = vec![];
        for (k,s) in documents {
            let v = D::from_str(s.as_str()).unwrap() as D;
            res.push((get_doc_name(k.as_str()),v));
        }
        trace!("fetch from storage - record count - {}",res.len());
        res
    }

    pub fn prefix(&self, k: String) -> Vec<(String, String)> {
        trace!("prefix scan for key {}",&k);
        self.tree
            .scan_prefix(k.as_bytes())
            .map(|kv|{
                let kv = kv.unwrap();
                let key = String::from_utf8(kv.0.to_vec()).unwrap();
                let value = String::from_utf8(kv.1.to_vec()).unwrap();
                (key, value)
            })
            .collect::<Vec<(String, String)>>()
    }

    #[allow(dead_code)]
    pub fn iter(&self) -> Iter {
        self.tree.iter()
    }

    pub async fn flush(&self) -> usize {
        let size = self.tree.flush_async().await;
        trace!("storage flushed");
        size.unwrap()
    }
}