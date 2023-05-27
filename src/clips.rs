use std::fmt::{Debug, Display};
use std::hash::Hash;
use dashmap::{DashMap, DashSet};
use dashmap::rayon::map::Iter;
use dashmap::mapref::one::{Ref};
use log::trace;
use rayon::prelude::*;
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
        Debug +
        Display +
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
        trace!("deleting clip key {}",&k);
        v.tokens().into_iter().for_each(|idx|{
            if let Some(set) = self.kv.get_mut(&idx) {
                set.value().remove(&k);
            }
        });
    }

    pub fn delete_inner(&self, vw: &str, k: &K) {
        let view = &set_view_name(vw);
        trace!("deleting view - inner {}",&view);
        if let Some(set) = self.kv.get_mut(view) {
            set.value().remove(&k);
        }
    }

    pub fn delete_clip(&self, clip: &str) {
        trace!("deleting clip {}",&clip);
        self.kv.remove(clip);
    }

    pub fn clear_keys(&self) {
        trace!("clearing keys for clips");
        let non_ref = self.kv.clone();
        for kv in non_ref {
            self.delete_clip(kv.0.as_str());
        }
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
}