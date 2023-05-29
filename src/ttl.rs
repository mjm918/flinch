use std::thread::sleep;
use bincode::{Decode, Encode};
use dashmap::DashMap;
use log::{trace};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use crate::ge::EVENT_EMITTER;

#[derive(Serialize, Deserialize, Debug, Clone, Encode, Decode)]
pub struct Entry {
    pub key: String,
    pub reg_at: i64
}

#[derive(Clone)]
pub struct Ttl {
    kv: DashMap<i64, Entry>,
    watching: String
}

impl Ttl {
    pub fn new(name: &str) -> Self {
        Self {
            kv: DashMap::new(),
            watching: format!("{}",name)
        }
    }

    pub fn push(&self, ttl: i64, key: String) {
        trace!("putting {} to TTL. Timestamp {}",&key, &ttl);
        self.kv.insert(ttl, Entry { key, reg_at: chrono::Local::now().timestamp() });
    }

    pub fn never(&self, key: String) -> i64 {
        let future = chrono::Local::now() + chrono::Duration::days(365 * 30);
        let ttl = future.timestamp();
        self.push(ttl.to_owned(), key);
        ttl
    }

    pub fn start(&self) {
        loop {
            self.purge();
            sleep(std::time::Duration::from_millis(500));
        }
    }

    fn purge(&self) {
        let now = chrono::Local::now().timestamp();
        trace!("todo check {} keys",self.kv.len());
        let keys = self.kv.iter().filter(|kv|{
            let key = kv.key();
            key.cmp(&now).is_le()
        }).map(|kv|kv.key().to_owned()).collect::<Vec<i64>>();
        trace!("TTL found {} keys",keys.len());
        for key in keys {
            let removed = self.kv.get(&key);
            if let Some(kv) = removed {
                EVENT_EMITTER.lock().unwrap().emit("expired",kv.value().to_owned());
            }
        }
    }

    pub fn remove(&self, key: String) {
        let tm = self.kv.par_iter()
            .find_any(|kv|{
                let entry = kv.value();
                entry.key.eq(&key)
            })
            .map(|kv|{
                let key = kv.key();
                let entry = kv.value();
                (key.to_owned(), entry.to_owned())
            });
        if tm.is_some() {
            let pair = tm.unwrap();
            let entry = pair.1;
            trace!("removing key {} registered at {}",&entry.key, &entry.reg_at);
            self.kv.remove(&pair.0);
        } else {
            trace!("tried to remove key {} but no result found",key);
        }
    }
}
