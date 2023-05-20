use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use sled::{Tree, Iter, IVec, Db};
use tokio::sync::mpsc::Sender;
use crate::doc::Document;

pub struct Bkp {
    tree: Tree
}

impl Bkp {
    pub fn new(db: &Db, name: &str) -> Self {
        let tree = db.open_tree(name).unwrap();
        Self { tree }
    }

    pub fn put<D>(&self, k: String, d: D) -> sled::Result<Option<IVec>> where
        D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static + Document {
        self.tree.insert(k,IVec::from(d.string().as_str()))
    }

    pub fn remove(&self, k: String) -> sled::Result<Option<IVec>> {
        self.tree.remove(k)
    }

    pub fn get(&self, k: String) -> sled::Result<Option<IVec>> {
        self.tree.get(k)
    }

    pub fn fetch<D>(&self) -> Vec<(String, D)> where
        D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static + Document {
        let mut res: Vec<(String, D)> = vec![];
        for item in &self.tree {
            let kv = item.unwrap();
            let k = String::from_utf8(kv.0.to_vec()).unwrap();
            let s = String::from_utf8(kv.1.to_vec()).unwrap();
            let v = D::from_str(s.as_str()).unwrap() as D;
            res.push((k,v));
        }
        res
    }

    pub fn iter(&self) -> Iter {
        self.tree.iter()
    }

    pub async fn flush(&self) -> usize {
        let size = self.tree.flush_async().await;
        size.unwrap()
    }
}