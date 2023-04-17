use std::collections::HashSet;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use dashmap::{DashMap, DashSet};
use dashmap::rayon::map::Iter;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::task::JoinHandle;

pub struct InvertedIndex<K> {
    pub kv: Arc<DashMap<String, DashSet<K>>>
}

impl<K> InvertedIndex<K>
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
        let mut res = DashSet::new();
        self.kv.par_iter().for_each(|rkv|{
            let kv = rkv.pair();
            let key = kv.0;
            let mut counter = Arc::new(Mutex::new(0));
            words.par_iter().for_each(|word|{
                let mut c = counter.lock().unwrap();
                let token = word.to_lowercase();
                if key.contains(token.as_str()) {
                    *c += 1;
                }
                if *c >= words.len() {
                    let dk = kv.1.iter().map(|r|r.key().to_owned()).collect::<Vec<K>>();
                    for k in dk {
                        if res.get(&k).is_none() {
                            res.insert(k);
                        }
                    }
                }
            });
        });
        res.into_iter().collect()
        /*let mut res = HashSet::new();
        for rkv in self.kv.iter() {
            let kv = rkv.pair();
            let key = kv.0;
            let mut counter = 0;
            for word in words.iter() {
                let token = word.to_lowercase();
                if key.contains(token.as_str()) {
                    counter += 1;
                }
            }
            if counter >= words.len() {
                let dk = kv.1.iter().map(|r|r.key().to_owned()).collect::<Vec<K>>();
                for k in dk {
                    if res.get(&k).is_none() {
                        res.insert(k);
                    }
                }
            }
        }
        res.into_iter().collect()*/
    }

    pub fn clear(&self) {
        self.kv.clear();
    }
}
