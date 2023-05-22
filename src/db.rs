use anyhow::{Result};
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sled::Db;
use crate::doc::{Document, ViewConfig};
use crate::err::{CollectionError};
use crate::col::{Collection};

#[derive(Serialize, Deserialize, Clone)]
pub struct CollectionOptions {
    pub name: String,
    pub index_opts: Vec<String>,
    pub search_opts: Vec<String>,
    pub view_opts: Vec<ViewConfig>,
    pub range_opts: Vec<String>,
    pub clips_opts: Vec<String>,
}

pub struct Database<D> where D: Document {
    storage: DashMap<String, Collection<D>>,
    persist: Db
}

impl<D> Database<D>
     where D: Serialize + DeserializeOwned + Clone + Send + 'static + Document + Sync
{
    pub fn init() -> Self {
        Self {
            storage: DashMap::new(),
            persist: sled::open("./dbs").unwrap()
        }
    }

    pub fn ls(&self) -> Vec<String> {
        self.storage.iter().map(|kv|kv.key().to_string()).collect::<Vec<String>>()
    }

    pub fn add(&self, opts: CollectionOptions) -> Result<(), CollectionError> {
        let name = &opts.name;
        if let Err(err) = self.exi(name.as_str()) {
            return Err(err);
        }
        self.storage.insert(name.clone(), Collection::<D>::new(&self.persist, opts));
        Ok(())
    }

    pub async fn drop(&self, name: &str) -> Result<(), CollectionError> {
        if let Err(err) = self.exi(name) {
            return Err(err);
        }
        let col = self.using(name);
        let col = col.unwrap();
            col.value().empty().await;
        self.storage.remove(name);

        Ok(())
    }

    pub fn using(&self, name: &str) -> Result<Ref<String,Collection<D>>, CollectionError> {
        if let Some(col) = self.storage.get(name) {
            return Ok(col);
        }
        Err(CollectionError::NoSuchCollection)
    }

    fn exi(&self, name: &str) -> Result<(), CollectionError> {
        if let Some(_) = self.storage.get(name) {
            return Err(CollectionError::DuplicateCollection);
        }
        Ok(())
    }
}
