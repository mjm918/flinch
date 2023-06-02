use std::fmt::{Debug, Display};
use std::hash::Hash;

use anyhow::Result;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use dashmap::rayon::map::Iter;
use log::trace;
use rayon::iter::IntoParallelRefIterator;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::doc_trait::Document;
use crate::errors::IndexError;

pub struct HashIndex<K> {
    pub kv: DashMap<String, K>,
}

impl<K> HashIndex<K>
    where K: Serialize +
    DeserializeOwned +
    PartialOrd +
    Ord +
    PartialEq +
    Eq +
    Hash +
    Clone +
    Send +
    Sync +
    Debug +
    Display +
    'static
{
    pub fn new() -> Self {
        Self {
            kv: DashMap::new()
        }
    }

    pub fn put<D>(&self, k: &K, v: &D) -> Result<(), (IndexError, String)> where D: Document {
        for key in v.keys().iter() {
            if let Some(_) = self.kv.get(key) {
                return Err((IndexError::DuplicateDocument, key.clone()));
            }
        }
        v.keys().into_iter().for_each(|idx| {
            self.kv.insert(idx, k.clone());
        });
        Ok(())
    }

    pub fn delete<D>(&self, v: &D) where D: Document {
        trace!("deleting hash index {:?}",&v.keys());
        v.keys().into_iter().for_each(|idx| {
            self.kv.remove(&idx);
        });
    }

    pub fn get(&self, idx: &str) -> Option<Ref<String, K>> {
        self.kv.get(idx)
    }

    pub fn iter(&self) -> Iter<'_, String, K> {
        self.kv.par_iter()
    }
}