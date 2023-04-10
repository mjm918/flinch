use anyhow::Result;
use std::hash::Hash;
use anymap::AnyMap;
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

pub struct Storage <K, D> where D: Document {
    pub name: String,
    pub store: Collection<K, D>,
    pub created_at: DateTime<Local>,
    pub created_by: Option<String>
}

pub struct Flinch {
    storage: AnyMap
}

impl Flinch {
    pub fn init() -> Self {
        Self {
            storage: AnyMap::new()
        }
    }
    pub fn create<K, D>(&mut self, opts: CollectionOptions) -> Result<(), CollectionError>
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
              D: Serialize + DeserializeOwned + Clone + Send + 'static + Document {
        if let Err(err) = self.store_exists::<K, D>(opts.name.as_str()) {
            return Err(err);
        }
        self.storage.insert(Storage{
            name: opts.name.clone(),
            store: Collection::<K, D>::new(opts),
            created_at: Local::now(),
            created_by: None,
        });
        Ok(())
    }

    pub fn using<K, D>(&self, name: &str) -> Result<&Collection<K, D>, CollectionError>
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
              D: Serialize + DeserializeOwned + Clone + Send + 'static + Document {
        if let Some(col) = self.storage.get::<Storage<K, D>>() {
            if col.name.as_str().eq(name) {
                return Ok(&col.store);
            }
        }
        Err(CollectionError::NoSuchCollection)
    }

    fn store_exists<K,D>(&self, name: &str) -> Result<(), CollectionError>
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
              D: Serialize + DeserializeOwned + Clone + Send + 'static + Document{
        if let Some(col) = self.storage.get::<Storage<K, D>>() {
            if col.name.as_str().eq(name) {
                return Err(CollectionError::DuplicateCollection);
            }
        }
        Ok(())
    }
}