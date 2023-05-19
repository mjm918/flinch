use anyhow::{Result};
use std::hash::Hash;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use crate::doc::{Document, ViewConfig};
use crate::err::{CollectionError};
use crate::col::{Collection};

#[derive(Serialize, Deserialize)]
pub struct CollectionOptions {
    pub name: Option<String>,
    pub index_opts: Vec<String>,
    pub search_opts: Vec<String>,
    pub view_opts: Vec<ViewConfig>,
    pub range_opts: Vec<String>,
    pub clips_opts: Vec<String>
}

pub struct Database<K, D> where D: Document {
    storage: DashMap<String, Collection<K, D>>
}

impl<K, D> Database<K, D>
    where K: Serialize
    + DeserializeOwned
    + PartialOrd
    + Ord
    + PartialEq
    + Eq
    + Hash
    + Clone
    + Send
    + Sync
    + 'static,
          D: Serialize + DeserializeOwned + Clone + Send + 'static + Document + Sync
{
    pub fn init() -> Self {
        Self {
            storage: DashMap::new()
        }
    }

    pub fn ls(&self) -> Vec<String> {
        self.storage.iter().map(|kv|kv.key().to_string()).collect::<Vec<String>>()
    }

    pub fn add(&self, opts: CollectionOptions) -> Result<(), CollectionError> {
        if opts.name.is_none() {
            return Err(CollectionError::OptionsProvidedAreNotValid);
        }
        let name = opts.name.as_ref().unwrap();
        if let Err(err) = self.exi(name.as_str()) {
            return Err(err);
        }
        self.storage.insert(name.clone(), Collection::<K, D>::new(opts));
        Ok(())
    }

    pub async fn drop_c(&self, name: &str) -> Result<(), CollectionError> {
        if let Err(err) = self.exi(name) {
            return Err(err);
        }
        let col = self.using(name);
        let col = col.unwrap();
            col.value().drop().await;
        self.storage.remove(name);

        Ok(())
    }

    pub fn using(&self, name: &str) -> Result<Ref<String,Collection<K, D>>, CollectionError> {
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
