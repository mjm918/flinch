use anyhow::Result;
use std::hash::Hash;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use serde::de::DeserializeOwned;
use serde::{Serialize};
use crate::doc::{Document, Field};
use crate::err::StoreError;
use crate::store::{Store};

pub struct StoreOptions {
    pub name: String,
    pub index_opts: Vec<String>,
    pub search_opts: Vec<String>,
    pub view_opts: Option<String>,
    pub range_opts: Vec<Field>,
    pub clips_opts: Vec<String>
}

pub struct Flinch<K, D> where D: Document {
    storage: DashMap<String, Store<K, D>>
}

impl<K, D> Flinch<K, D>
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
      D: Serialize + DeserializeOwned + Clone + Send + 'static + Document
{
    pub fn init() -> Self {
        Self {
            storage: DashMap::new()
        }
    }
    pub fn create(&self, opts: StoreOptions) -> Result<(), StoreError> {
        if let Err(err) = self.store_exists(opts.name.as_str()) {
            return Err(err);
        }
        self.storage.insert(opts.name.clone(),Store::new(opts));
        Ok(())
    }

    pub fn using(&self, name: &str) -> Ref<String, Store<K, D>> {
        self.storage.get(name).unwrap()
    }

    fn store_exists(&self, name: &str) -> Result<(), StoreError> {
        if let Some(_s) = self.storage.get(name) {
            return Err(StoreError::DuplicateStore);
        }
        Ok(())
    }
}