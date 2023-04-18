use anyhow::Result;
use std::hash::Hash;
use chrono::{DateTime, Local};
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use serde::de::DeserializeOwned;
use serde::{Serialize};
use crate::doc::{Document, ViewConfig};
use crate::err::CollectionError;
use crate::col::{Collection};

pub struct CollectionOptions {
    pub name: String,
    pub index_opts: Vec<String>,
    pub search_opts: Vec<String>,
    pub view_opts: Vec<ViewConfig>,
    pub range_opts: Vec<String>,
    pub clips_opts: Vec<String>
}

pub struct Database<K, D> where D: Document {
    storage: DashMap<String, Collection<K, D>>
}

impl<K, D> Database<K,D>
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
    pub fn create(&mut self, opts: CollectionOptions) -> Result<(), CollectionError> {
        if let Err(err) = self.store_exists(opts.name.as_str()) {
            return Err(err);
        }
        self.storage.insert(opts.name.clone(), Collection::<K, D>::new(opts));
        Ok(())
    }

    pub fn using(&self, name: &str) -> Result<Ref<String, Collection<K, D>>, CollectionError> {
        if let Some(col) = self.storage.get(name) {
            return Ok(col);
        }
        Err(CollectionError::NoSuchCollection)
    }

    fn store_exists(&self, name: &str) -> Result<(), CollectionError> {
        if let Some(_) = self.storage.get(name) {
            return Err(CollectionError::DuplicateCollection);
        }
        Ok(())
    }
}