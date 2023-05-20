use anyhow::Result;
use std::hash::Hash;
use crossbeam_queue::{ArrayQueue};
use dashmap::{DashMap, DashSet};
use dashmap::rayon::map::Iter;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use sled::Db;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
use crate::bkp::Bkp;
use crate::clips::Clips;
use crate::doc::Document;
use crate::db::CollectionOptions;
use crate::err::{IndexError};
use crate::hdrs::{PubSubEvent, ActionType, PubSubRes, FuncResult, FuncType};
use crate::hidx::HashIndex;
use crate::ividx::InvertedIndex;
use crate::range::Range;
use crate::pbsb::PubSub;
use crate::utils::ExecTime;
use crate::wtch::Watchman;

pub struct Collection<D: Document> {
    bkp: Bkp,
    kv: DashMap<String, D>,
    hash_idx: HashIndex<String>,
    inverted_idx: InvertedIndex<String>,
    clips: Clips<String>,
    range: Range<String>,
    watchman: PubSub<PubSubEvent<String, D>>,
    pub opts: CollectionOptions
}
pub type ExecutionTime = String;
pub type K = String;
impl<D> Collection< D>
where
    D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static + Document
{
    pub fn new(db: &Db, opts: CollectionOptions) -> Self {
        let option = opts.clone();
        Self {
            bkp: Bkp::new(&db, option.name.unwrap().as_str()),
            kv: DashMap::new(),
            hash_idx: HashIndex::new(),
            inverted_idx: InvertedIndex::new(),
            clips: Clips::new(),
            range: Range::new(),
            watchman: Watchman::<PubSubEvent<K, D>>::new(vec![]).unwrap().start(),
            opts
        }
    }

    pub async fn load_bkp(&self) {
        let res = self.bkp.fetch();
        for kv in res {
            let _ = self.put_internal(kv.0, kv.1, false).await;
        }
    }

    pub async fn sub(&self, sx: Sender<PubSubEvent<K,D>>) -> Result<(), PubSubRes> {
        let _ = self.watchman.notify(PubSubEvent::Subscribed(sx.clone())).await;
        self.watchman.reg(sx).await
    }

    pub async fn put(&self, k: K, d: D) -> Result<ExecutionTime, IndexError> {
        self.put_internal(k, d, true).await
    }

    async fn put_internal(&self, k: K, d: D, bkp: bool) -> Result<ExecutionTime, IndexError> {
        let exec = ExecTime::new();
        let mut v = d;
        v.set_opts(&self.opts);

        let query = ActionType::Insert(k.to_string(), v.clone());
        let _ = self.watchman.notify(PubSubEvent::Data(query)).await;

        if !self.opts.index_opts.is_empty() {
            if let Err(_err) = self.hash_idx.put(&k, &v) {
                return Err(IndexError::DuplicateDocument);
            }
        }
        if !self.opts.view_opts.is_empty() {
            if let Some(vw) = v.binding() {
                self.clips.put_view(&vw, &k);
            }
        }
        if !self.opts.search_opts.is_empty() {
            if let Some(val) = v.content() {
                let _ = self.inverted_idx.put(k.clone(), val).await;
            }
        }
        if !self.opts.clips_opts.is_empty() {
            self.clips.put(&k, &v);
        }
        if !self.opts.range_opts.is_empty() {
            self.range.put(&k, &v);
        }
        self.kv.insert(k.to_string(), v.clone());
        if bkp {
            let _ = self.bkp.put(k.to_string(),v);
        }
        Ok(exec.done())
    }

    pub async fn delete(&self, k: K) -> ExecutionTime {
        let exec = ExecTime::new();
        if self.kv.contains_key(&k) {
            let query = ActionType::<K,D>::Remove(k.clone());
            let _ = self.watchman.notify(PubSubEvent::Data(query)).await;

            let (_, v) = self.kv.remove(&k).unwrap();

            self.hash_idx.delete(&v.clone());
            if let Some(view) = v.binding() {
                self.clips.delete_inner(&view, &k)
            }
            if let Some(content) = v.content() {
                let _ = self.inverted_idx.delete(k.to_string(), content).await;
            }
            self.clips.delete(&k, &v);
            let _ = self.bkp.remove(k.to_string());
        }
        exec.done()
    }

    pub async fn delete_by_range(&self, field: &str, from: String, to: String) -> ExecutionTime {
        let exec = ExecTime::new();
        let res = self.fetch_range(field, from, to).data;
        for kv in res.iter() {
            self.delete(kv.0.clone()).await;
        }
        exec.done()
    }

    pub async fn delete_by_clip(&self, clip: &str) -> ExecutionTime {
        let exec = ExecTime::new();
        let res = self.fetch_clip(clip).data;
        for kv in res.iter() {
            self.delete(kv.0.clone()).await;
        }
        exec.done()
    }

    pub fn multi_get(&self, keys: Vec<&K>) -> FuncResult<Vec<(K, D)>> {
        let exec = ExecTime::new();
        let mut res = Vec::with_capacity(keys.len());
        keys.iter().for_each(|k|{
            if let Some(v) = self.kv.get(k.clone()){
                let pair = v.pair();
                res.push((pair.0.clone(), pair.1.clone()));
            }
        });
        FuncResult {
            query: FuncType::LookupMulti,
            data: res,
            time_taken: exec.done()
        }
    }

    pub fn fetch_range(&self, field: &str, from: String, to: String) -> FuncResult<Vec<(K, D)>> {
        let exec = ExecTime::new();
        let mut res = Vec::new();
        let q = format!("field {} from {} to {}", &field, &from, &to);
        for k in self.range.range(field, from, to) {
            if let Some(v) = self.kv.get(&k) {
                res.push((k.to_string(), v.clone()));
            }
        }
        FuncResult {
            query: FuncType::FetchRange(q),
            data: res,
            time_taken: exec.done(),
        }
    }

    pub fn get(&self, k: &K) -> FuncResult<Option<(K, D)>> {
        let exec = ExecTime::new();
        let res = if let Some(res) = self.kv.get(k) {
            let pair = res.pair();
            Some((pair.0.clone(), pair.1.clone()))
        } else {
            None
        };
        FuncResult {
            query: FuncType::Lookup,
            data: res,
            time_taken: exec.done()
        }
    }

    pub fn get_index(&self, index: &str) -> FuncResult<Option<(K, D)>> {
        let exec = ExecTime::new();
        let res = match self.hash_idx.get(index) {
            None => None,
            Some(v) => {
                let kv = self.kv.get(v.value());
                if kv.is_some() {
                    let kv = kv.unwrap();
                    let pair = kv.pair();
                    Some((pair.0.clone(), pair.1.clone()))
                } else {
                    None
                }
            }
        };
        FuncResult {
            query: FuncType::LookupIndex(index.to_string()),
            data: res,
            time_taken: exec.done()
        }
    }

    pub fn fetch_clip(&self, clip: &str) -> FuncResult<Vec<(K, D)>> {
        let exec = ExecTime::new();
        let res = match self.clips.get(clip) {
            Some(v) => {
                let mut res = Vec::with_capacity(v.value().len());
                for k in v.value().iter() {
                    if let Some(kv) = self.kv.get(&k.clone()) {
                        let pair = kv.pair();
                        res.push((pair.0.clone(), pair.1.clone()));
                    }
                }
                res
            }
            None => vec![]
        };
        FuncResult {
            query: FuncType::FetchClip(clip.to_string()),
            data: res,
            time_taken: exec.done()
        }
    }

    pub fn fetch_view(&self, view_name: &str) -> FuncResult<Vec<(K, Value)>> {
        let exec = ExecTime::new();
        let res = match self.clips.get_view(view_name) {
            Some(v) => {
                let mut res = Vec::with_capacity(v.value().len());
                for k in v.value().iter() {
                    if let Some(kd) = self.kv.get(&k.clone()) {
                        let pair = kd.pair();
                        res.push((pair.0.clone(), pair.1.document().clone()));
                    }
                }
                res
            }
            None => vec![]
        };
        FuncResult {
            query: FuncType::FetchView(view_name.to_string()),
            data: res,
            time_taken: exec.done(),
        }
    }

    pub fn search(&self, query: &str) -> FuncResult<Vec<(K, D)>> {
        let text = query.to_string();
        let exec = ExecTime::new();
        let words: Vec<&str> = text.split_whitespace().collect();
        let keys = self.inverted_idx.find(words);
        let mut res = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(kv) = self.kv.get(&key) {
                let pair = kv.pair();
                res.push((pair.0.clone(), pair.1.clone()));
            }
        }
        FuncResult {
            query: FuncType::Lookup,
            data: res,
            time_taken: exec.done()
        }
    }

    pub fn like_search(&self, query: &str) -> FuncResult<ArrayQueue<(K, D)>> {
        let text = query.to_string();
        let exec = ExecTime::new();
        let words: Vec<&str> = text.split_whitespace().collect();
        let keys = self.inverted_idx.w_find(words);
        if keys.len() > 0 {
            let res = ArrayQueue::new(keys.len());
            keys.par_iter().for_each(|key|{
                if let Some(kv) = self.kv.get(key) {
                    let pair = kv.pair();
                    let _ = res.push((pair.0.clone(), pair.1.clone()));
                }
            });
            FuncResult {
                query: FuncType::LikeSearch(text),
                data: res,
                time_taken: exec.done()
            }
        } else {
            FuncResult {
                query: FuncType::LikeSearch(text),
                data: ArrayQueue::new(1),
                time_taken: exec.done()
            }
        }
    }

    pub fn iter(&self) -> Iter<'_, K, D> {
        self.kv.par_iter()
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

    pub async fn drop_c(&self) {
        let local = self.kv.clone();
        for kv in local {
            self.delete(kv.0).await;
        }
    }

    pub async fn flush_bkp(&self) -> usize {
        self.bkp.flush().await
    }
}
