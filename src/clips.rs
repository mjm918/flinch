use std::hash::Hash;
use dashmap::{DashMap, DashSet};
use dashmap::rayon::map::Iter;
use dashmap::mapref::one::{Ref};
use rayon::prelude::IntoParallelRefIterator;
use serde::de::DeserializeOwned;
use serde::Serialize;
use crate::doc::Document;
use crate::utils::set_view_name;

pub struct Clips<K> {
    pub kv: DashMap<String, DashSet<K>>
}

impl<K> Clips<K>
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
        'static
{
    pub fn new() -> Self {
        Self {
            kv: DashMap::new()
        }
    }

    pub fn put<D>(&self, k: &K, v: &D) where D: Document {
        v.tokens().into_iter().for_each(|idx|{
            match self.kv.get_mut(&idx) {
                None => {
                    let set = DashSet::new();
                    set.insert(k.clone());
                    self.kv.insert(idx, set);
                }
                Some(set) => {
                    set.value().insert(k.clone());
                }
            }
        });
    }

    pub fn put_view(&self, vw: &str, k: &K) {
        let view = set_view_name(vw);
        match self.kv.get_mut(&view) {
            None => {
                let set = DashSet::new();
                set.insert(k.clone());
                self.kv.insert(view, set);
            }
            Some(set) => {
                set.value().insert(k.clone());
            }
        }
    }

    pub fn delete<D>(&self, k: &K, v: &D) where D: Document {
        v.tokens().into_iter().for_each(|idx|{
            if let Some(set) = self.kv.get_mut(&idx) {
                set.value().remove(&k);
            }
        });
    }

    pub fn delete_inner(&self, vw: &str, k: &K) {
        let view = &set_view_name(vw);
        if let Some(set) = self.kv.get_mut(view) {
            set.value().remove(&k);
        }
    }

    pub fn delete_clip(&self, clip: &str) {
        self.kv.remove(clip);
    }

    pub fn delete_view(&self, vw: &str) {
        let view = set_view_name(vw);
        self.kv.remove(&view);
    }

    pub fn get(&self, clip: &str) -> Option<Ref<String, DashSet<K>>> {
        self.kv.get(clip)
    }

    pub fn get_view(&self, vw: &str) -> Option<Ref<String, DashSet<K>>> {
        self.kv.get(&set_view_name(vw))
    }

    pub fn iter(&self) -> Iter<String, DashSet<K>> {
        self.kv.par_iter()
    }

    pub fn clear(&self) {
        self.kv.clear();
        self.kv.shrink_to_fit();
    }
}