use anyhow::Result;
use std::hash::Hash;
use dashmap::{DashMap, DashSet};
use dashmap::iter::Iter;
use dashmap::mapref::one::Ref;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
use crate::clips::Clips;
use crate::doc::Document;
use crate::flinch::CollectionOptions;
use crate::hdrs::{Event, ExecTime, Query, SessionRes};
use crate::hidx::HIdx;
use crate::ividx::IvIdx;
use crate::range::Range;
use crate::sess::Session;
use crate::wtch::Wtch;

pub struct Collection<K, D: Document> {
    kv: DashMap<K, D>,
    hidx: HIdx<K>,
    ividx: IvIdx<K>,
    clips: Clips<K>,
    range: Range<K>,
    watcher: Session<Event<K, D>>,
    opts: CollectionOptions
}
pub type ExecutionTime = String;

impl<K,D> Collection<K, D>
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
    pub fn new(opts: CollectionOptions) -> Self {
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
        let _ = self.watcher.notify(Event::Subscribed(sx.clone())).await;
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
        if let Some(vw) = v.binding() {
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

    pub async fn delete(&self, k: K) -> ExecutionTime {
        let exec = ExecTime::new();
        if self.kv.contains_key(&k) {
            let (_, v) = self.kv.remove(&k).unwrap();
            let query = Query::<K,D>::Remove(k.clone());
            let _ = self.watcher.notify(Event::Query(query)).await;

            self.hidx.delete(&v.clone());
            if let Some(view) = v.binding() {
                self.clips.delete_inner(&view, &k)
            }
            if let Some(content) = v.content() {
                let _ = self.ividx.delete(k.clone(), content).await;
            }
            self.clips.delete(&k, &v);
        }
        exec.done()
    }

    pub async fn delete_by_range(&self, field: &str, from: String, to: String) -> ExecutionTime {
        let exec = ExecTime::new();
        let res = self.range(field, from, to);
        for kv in res.1.iter() {
            self.delete(kv.clone().0).await;
        }
        exec.done()
    }

    pub async fn delete_by_clip(&self, clip: &str) -> ExecutionTime {
        let exec = ExecTime::new();
        let res = self.get_clip(clip);
        for kv in res.1 {
            self.delete(kv.0.clone()).await;
        }
        exec.done()
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

    pub fn range(&self, field: &str, from: String, to: String) -> (ExecutionTime, Vec<(K, D)>) {
        let exec = ExecTime::new();
        let mut res = Vec::new();
        for k in self.range.range(field, from, to) {
            if let Some(v) = self.kv.get(&k) {
                res.push((k.clone(), v.clone()));
            }
        }
        (exec.done(), res)
    }

    pub fn get(&self, k: &K) -> (ExecutionTime, Option<Ref<K, D>>) {
        let exec = ExecTime::new();
        (exec.done(), self.kv.get(k))
    }

    pub fn get_idx(&self, idx: &str) -> (ExecutionTime, Option<Ref<K, D>>) {
        let exec = ExecTime::new();
        let res = match self.hidx.get(idx) {
            None => None,
            Some(v) => {
                self.kv.get(v.value())
            }
        };
        (exec.done(), res)
    }

    pub fn get_clip(&self, clip: &str) -> (ExecutionTime, Vec<(K, D)>) {
        let exec = ExecTime::new();
        let res = match self.clips.get(clip) {
            Some(v) => {
                let mut res = Vec::with_capacity(v.value().len());
                for k in v.value().iter() {
                    if let Some(kv) = self.kv.get(&k) {
                        res.push((k.clone(), kv.clone()));
                    }
                }
                res
            }
            None => vec![]
        };
        (exec.done(), res)
    }

    pub fn fetch_view(&self, view_name: &str) -> (ExecutionTime, Vec<(K, D)>) {
        let exec = ExecTime::new();
        let res = match self.clips.get_view(view_name) {
            Some(v) => {
                let mut res = Vec::with_capacity(v.value().len());
                for k in v.value().iter() {
                    if let Some(kd) = self.kv.get(&k) {
                        res.push((kd.key().clone(), kd.value().clone()));
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

    pub fn len(&self) -> usize {
        self.kv.len()
    }

    pub fn id(&self) -> String {
        Uuid::new_v4().as_hyphenated().to_string()
    }
}
