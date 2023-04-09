use std::hash::Hash;

use anyhow::Result;
use dashmap::DashMap;
use dashmap::iter::Iter;
use dashmap::mapref::one::Ref;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::doc::Document;
use crate::err::IndexError;

pub struct HIdx<K> {
    pub kv: DashMap<String, K>,
}

impl<K> HIdx<K>
    where K: Serialize +
    DeserializeOwned +
    PartialOrd +
    Ord +
    PartialEq +
    Eq +
    Hash +
    Clone +
    Send +
    'static
{
    pub fn new() -> Self {
        Self {
            kv: DashMap::new()
        }
    }

    pub fn put<D>(&self, k: &K, v: &D) -> Result<(), IndexError> where D: Document {
        for key in v.keys().iter() {
            if let Some(_) = self.kv.get(key) {
                return Err(IndexError::DuplicateDocument);
            }
        }
        v.keys().into_iter().for_each(|idx| {
            self.kv.insert(idx, k.clone());
        });
        Ok(())
    }

    pub fn delete<D>(&self, v: &D) where D: Document {
        v.keys().into_iter().for_each(|idx| {
            self.kv.remove(&idx);
        });
    }

    pub fn seek(&self, idx: &str) -> Option<Ref<String, K>> {
        self.kv.get(idx)
    }

    pub fn iter(&self) -> Iter<String, K> {
        self.kv.iter()
    }
}