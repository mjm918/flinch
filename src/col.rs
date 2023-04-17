use anyhow::Result;
use std::hash::Hash;
use dashmap::{DashMap, DashSet};
use dashmap::iter::Iter;
use dashmap::mapref::multiple::RefMulti;
use dashmap::mapref::one::Ref;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
use crate::clips::Clips;
use crate::doc::Document;
use crate::db::CollectionOptions;
use crate::err::QueryError;
use crate::hdrs::{Event, ActionType, SessionRes};
use crate::hidx::HashIndex;
use crate::ividx::InvertedIndex;
use crate::range::Range;
use crate::sess::Session;
use crate::utils::ExecTime;
use crate::wtch::Watchman;

pub struct Collection<K, D: Document> {
    kv: DashMap<K, D>,
    hash_idx: HashIndex<K>,
    inverted_idx: InvertedIndex<K>,
    clips: Clips<K>,
    range: Range<K>,
    watchman: Session<Event<K, D>>,
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
        Self {
            kv: DashMap::new(),
            hash_idx: HashIndex::new(),
            inverted_idx: InvertedIndex::new(),
            clips: Clips::new(),
            range: Range::new(),
            watchman: Watchman::<Event<K, D>>::new(vec![]).unwrap().start(),
            opts
        }
    }

    pub async fn sub(&self, sx: Sender<Event<K,D>>) -> Result<(), SessionRes> {
        let _ = self.watchman.notify(Event::Subscribed(sx.clone())).await;
        self.watchman.reg(sx).await
    }

    pub async fn put(&self, k: K, d: D) -> Result<ExecutionTime, SessionRes> {
        let exec = ExecTime::new();
        let mut v = d;
        v.set_opts(&self.opts);

        let query = ActionType::Insert(k.clone(), v.clone());
        let _ = self.watchman.notify(Event::Data(query)).await;

        if let Err(err) = self.hash_idx.put(&k, &v) {
            return Err(SessionRes::Err(err.to_string()));
        }
        if let Some(vw) = v.binding() {
            self.clips.put_view(&vw, &k);
        }
        if let Some(val) = v.content() {
            let _ = self.inverted_idx.put(k.clone(), val).await;
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
            let query = ActionType::<K,D>::Remove(k.clone());
            let _ = self.watchman.notify(Event::Data(query)).await;

            self.hash_idx.delete(&v.clone());
            if let Some(view) = v.binding() {
                self.clips.delete_inner(&view, &k)
            }
            if let Some(content) = v.content() {
                let _ = self.inverted_idx.delete(k.clone(), content).await;
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
        let res = match self.hash_idx.get(idx) {
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
        let keys = self.inverted_idx.find(words);
        let mut res = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(kv) = self.kv.get(&key) {
                res.push(kv);
            }
        }
        (exec.done(), res)
    }

    pub fn like_search(&self, text: String) -> (ExecutionTime, Vec<Ref<K, D>>) {
        let exec = ExecTime::new();
        let words: Vec<&str> = text.split_whitespace().collect();
        let keys = self.inverted_idx.w_find(words);
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
        self.hash_idx.iter()
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

    pub fn drop(&self) {
        self.hash_idx.clear();
        self.inverted_idx.clear();
        self.range.clear();
        self.clips.clear();
        self.kv.clear();
    }

    pub fn query(&self, cmd: &str) -> Result<Vec<RefMulti<K, D>>, QueryError> {
        let mut res = vec![];
        for it in self.iter() {
            let chk = it.value().document().pointer(cmd);
            if chk.is_some() && chk.unwrap().as_i64().unwrap() < 10 {
                res.push(it);
            }
        }
        Ok(res)
    }
}
