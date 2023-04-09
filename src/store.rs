use anyhow::Result;
use std::hash::Hash;
use dashmap::{DashMap, DashSet};
use dashmap::iter::Iter;
use dashmap::mapref::one::Ref;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use crate::clips::Clips;
use crate::doc::Document;
use crate::flinch::StoreOptions;
use crate::hdrs::{Event, ExecTime, Query, SessionRes};
use crate::hidx::HIdx;
use crate::ividx::IvIdx;
use crate::range::Range;
use crate::sess::Session;
use crate::wtch::Wtch;

pub struct Store<K, D: Document> {
    kv: DashMap<K, D>,
    hidx: HIdx<K>,
    ividx: IvIdx<K>,
    clips: Clips<K>,
    range: Range<K>,
    watcher: Session<Event<K, D>>,
    opts: StoreOptions
}
pub type ExecutionTime = String;

impl<K,D> Store<K, D>
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
    D: Serialize + DeserializeOwned + Clone + Send + 'static + Document
{
    pub fn new(opts: StoreOptions) -> Self {
        Self{
            kv: DashMap::new(),
            hidx: HIdx::new(),
            ividx: IvIdx::new(),
            clips: Clips::new(),
            range: Range::new(),
            watcher: Wtch::<Event<K, D>>::new(vec![]).unwrap().start(),
            opts
        }
    }

    pub async fn sub(&self, sx: Sender<Event<K,D>>) -> Result<(), SessionRes> {
        self.watcher.notify(Event::Subscribed(sx.clone()));
        self.watcher.reg(sx).await
    }

    pub async fn put(&self, k: K, d: D) -> Result<ExecutionTime, SessionRes> {
        let exec = ExecTime::new();
        let mut v = d;
        v.set_opts(&self.opts);

        let query = Query::Insert(k.clone(), v.clone());
        let _ = self.watcher.notify(Event::Query(query)).await;
        if let Err(err) = self.hidx.put(&k, &v) {
            return Err(SessionRes::Err(err.to_string()));
        }
        if let Some(vw) = v.filter() {
            self.clips.put_view(&vw, &k);
        }
        if let Some(val) = v.content() {
            let _ = self.ividx.put(k.clone(), val).await;
        }
        self.clips.put(&k, &v);
        self.range.put(&k, &v);
        self.kv.insert(k, v);

        Ok(exec.done())
    }

    pub async fn delete(&self, k: K) -> Result<ExecutionTime, SessionRes> {
        let exec = ExecTime::new();
        match self.kv.get(&k) {
            None => return Ok(exec.done()),
            Some(v) => {
                let query = Query::<K,D>::Remove(k.clone());
                let _ = self.watcher.notify(Event::Query(query)).await;

                self.hidx.delete(v.value());
                if let Some(view) = v.filter() {
                    self.clips.delete_inner(&view, &k)
                }
                if let Some(content) = v.content() {
                    let _ = self.ividx.delete(k.clone(), content).await;
                }
                self.clips.delete(&k, v.value());
                self.range.delete(&k, v.value());
            }
        }
        self.kv.remove(&k);

        Ok(exec.done())
    }

    pub fn multi_get(&self, keys: Vec<&K>) -> (ExecutionTime, Vec<Ref<K, D>>) {
        let exec = ExecTime::new();
        let mut res = Vec::with_capacity(keys.len());
        keys.iter().for_each(|k|{
            if let Some(v) = self.kv.get(k){
                res.push(v);
            }
        });
        (exec.done(), res)
    }

    pub fn range(&self, field: &str, from: String, to: String) -> (ExecutionTime, Vec<Ref<K, D>>) {
        let exec = ExecTime::new();
        let mut res = Vec::new();
        for k in self.range.range(field, from, to) {
            if let Some(v) = self.kv.get(&k) {
                res.push(v);
            }
        }
        (exec.done(), res)
    }

    pub fn seek(&self, k: &K) -> (ExecutionTime, Option<Ref<K, D>>) {
        let exec = ExecTime::new();
        (exec.done(), self.kv.get(k))
    }

    pub fn seek_idx(&self, idx: &str) -> (ExecutionTime, Option<Ref<K, D>>) {
        let exec = ExecTime::new();
        let res = match self.hidx.seek(idx) {
            None => None,
            Some(v) => {
                self.kv.get(v.value())
            }
        };
        (exec.done(), res)
    }

    pub fn seek_clip(&self, clip: &str) -> (ExecutionTime, Vec<Ref<K, D>>) {
        let exec = ExecTime::new();
        let res = match self.clips.seek(clip) {
            Some(v) => {
                let mut res = Vec::with_capacity(v.value().len());
                for k in v.value().iter() {
                    if let Some(kv) = self.kv.get(&k) {
                        res.push(kv);
                    }
                }
                res
            }
            None => vec![]
        };
        (exec.done(), res)
    }

    pub fn fetch_view(&self, view_name: &str) -> (ExecutionTime, Vec<Ref<K, D>>) {
        let exec = ExecTime::new();
        let res = match self.clips.seek_view(view_name) {
            Some(v) => {
                let mut res = Vec::with_capacity(v.value().len());
                for k in v.value().iter() {
                    if let Some(kd) = self.kv.get(&k) {
                        res.push(kd);
                    }
                }
                res
            }
            None => vec![]
        };
        (exec.done(), res)
    }

    pub fn search(&self, text: String) -> (ExecutionTime, Vec<Ref<K, D>>) {
        let exec = ExecTime::new();
        let words: Vec<&str> = text.split_whitespace().collect();
        let keys = self.ividx.find(words);
        let mut res = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(kv) = self.kv.get(&key) {
                res.push(kv);
            }
        }
        (exec.done(), res)
    }

    pub fn wildcard_search(&self, text: String) -> (ExecutionTime, Vec<Ref<K, D>>) {
        let exec = ExecTime::new();
        let words: Vec<&str> = text.split_whitespace().collect();
        let keys = self.ividx.w_find(words);
        let mut res = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(kv) = self.kv.get(&key) {
                res.push(kv);
            }
        }
        (exec.done(), res)
    }

    pub fn iter(&self) -> Iter<'_, K, D> {
        self.kv.iter()
    }

    pub fn iter_index(&self) -> Iter<'_, String, K> {
        self.hidx.iter()
    }

    pub fn iter_clips(&self) -> Iter<String, DashSet<K>> {
        self.clips.iter()
    }

    pub fn len(self) -> usize {
        self.kv.len()
    }
}
