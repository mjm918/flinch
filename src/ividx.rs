use std::collections::HashSet;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use rayon::prelude::*;

use dashmap::{DashMap, DashSet};
use tokio::task::JoinHandle;

pub struct IvIdx<K> {
    pub kv: Arc<DashMap<String, DashSet<K>>>
}

impl<K> IvIdx<K>
    where K: PartialOrd
    +  Ord
    +  PartialEq
    +  Eq
    +  Hash
    +  Clone
    +  Send
    +  Sync
    + 'static
{
    pub fn new() -> Self {
        Self {
            kv: Arc::new(DashMap::new())
        }
    }

    pub fn put(&self, k: K, v: String) -> JoinHandle<()> {
        let rf = self.kv.clone();
        tokio::spawn(async move {
            for w in v.split_whitespace() {
                let token = w.to_lowercase();
                let key = &k;
                match rf.get_mut(&token) {
                    None => {
                        let set = DashSet::new();
                        set.insert(key.to_owned());
                        rf.insert(token, set);
                    }
                    Some(set) => {
                        if set.value().get(&key).is_none() {
                            set.value().insert(key.to_owned());
                        }
                    }
                }
            }
        })
    }

    pub fn delete(&self, k: K, v: String) -> JoinHandle<()> {
        let rf = self.kv.clone();
        tokio::spawn(async move {
            for w in v.split_whitespace() {
                let token = w.to_lowercase();
                if let Some(set) = rf.get_mut(&token) {
                    set.value().remove(&k);
                }
            }
        })
    }

    pub fn find(&self, words: Vec<&str>) -> Vec<K> {
        let mut res = HashSet::new();
        for w in words {
            let token = w.to_lowercase();
            let dk = match self.kv.get(&token) {
                None => {
                    vec![]
                }
                Some(set) => {
                    set.value().iter().map(|r| r.key().to_owned()).collect()
                }
            };
            for k in dk {
                if res.get(&k).is_none() {
                    res.insert(k);
                }
            }
        }
        res.into_iter().collect()
    }

    pub fn w_find(&self, words: Vec<&str>) -> Vec<K> {
        let res = Arc::new(Mutex::new(HashSet::new()));
        words.par_iter().for_each(|w|{
            let token = w.to_lowercase();
            for kv in self.kv.iter() {
                let pair = kv.pair();
                if pair.0.contains(&token) {
                    let dk = pair.1.iter().map(|r|{
                       return r.key().to_owned();
                    }).collect::<Vec<K>>();
                    if dk.len() > 0 {
                        for k in dk {
                            if res.lock().unwrap().get(&k).is_none() {
                                res.lock().unwrap().insert(k);
                            }
                        }
                    }
                }
            }
        });
        let x = res.lock().unwrap().clone().into_iter().collect::<Vec<K>>(); x
    }
}
