use anyhow::{Error, Result};
use std::hash::Hash;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::doc::{Document, ViewConfig};
use crate::err::{CollectionError};
use crate::col::{Collection};
use crate::docv::QueryBased;
use crate::exec::execute;
use crate::hdrs::QueryResult;

#[derive(Serialize, Deserialize)]
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

    pub fn add(&self, opts: CollectionOptions) -> Result<(), CollectionError> {
        if let Err(err) = self.exi(opts.name.as_str()) {
            return Err(err);
        }
        self.storage.insert(opts.name.clone(), Collection::<K, D>::new(opts));
        Ok(())
    }

    pub async fn drop(&self, name: &str) -> Result<(), CollectionError> {
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

pub struct DatabaseWithQuery {
    db: Database<String, QueryBased>
}

impl DatabaseWithQuery {
    pub fn new() -> Self {
        Self {
            db: Database::init()
        }
    }

    pub fn query(&self, ql: &str) -> QueryResult {
        execute(&self.db.storage, ql)
    }
}