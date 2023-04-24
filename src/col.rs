use anyhow::Result;
use std::hash::Hash;
use crossbeam_queue::{ArrayQueue, SegQueue};
use dashmap::{DashMap, DashSet};
use dashmap::rayon::map::Iter;
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;
use crate::clips::Clips;
use crate::doc::Document;
use crate::db::CollectionOptions;
use crate::err::QueryError;
use crate::hdrs::{Event, ActionType, SessionRes, QueryResult, QueryType};
use crate::hidx::HashIndex;
use crate::ividx::InvertedIndex;
use crate::qry::Query;
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
    D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static + Document
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

    pub fn multi_get(&self, keys: Vec<&K>) -> QueryResult<Vec<(K, D)>> {
        let exec = ExecTime::new();
        let mut res = Vec::with_capacity(keys.len());
        keys.iter().for_each(|k|{
            if let Some(v) = self.kv.get(k){
                let pair = v.pair();
                res.push((pair.0.clone(), pair.1.clone()));
            }
        });
        QueryResult {
            query: QueryType::LookupMulti,
            data: res,
            time_taken: exec.done()
        }
    }

    pub fn fetch_range(&self, field: &str, from: String, to: String) -> QueryResult<Vec<(K, D)>> {
        let exec = ExecTime::new();
        let mut res = Vec::new();
        let q = format!("field {} from {} to {}", &field, &from, &to);
        for k in self.range.range(field, from, to) {
            if let Some(v) = self.kv.get(&k) {
                res.push((k.clone(), v.clone()));
            }
        }
        QueryResult {
            query: QueryType::FetchRange(q),
            data: res,
            time_taken: exec.done(),
        }
    }

    pub fn get(&self, k: &K) -> QueryResult<Option<(K, D)>> {
        let exec = ExecTime::new();
        let res = if let Some(res) = self.kv.get(k) {
            let pair = res.pair();
            Some((pair.0.clone(), pair.1.clone()))
        } else {
            None
        };
        QueryResult {
            query: QueryType::Lookup,
            data: res,
            time_taken: exec.done()
        }
    }

    pub fn get_index(&self, index: &str) -> QueryResult<Option<(K, D)>> {
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
        QueryResult {
            query: QueryType::LookupIndex(index.to_string()),
            data: res,
            time_taken: exec.done()
        }
    }

    pub fn fetch_clip(&self, clip: &str) -> QueryResult<Vec<(K, D)>> {
        let exec = ExecTime::new();
        let res = match self.clips.get(clip) {
            Some(v) => {
                let mut res = Vec::with_capacity(v.value().len());
                for k in v.value().iter() {
                    if let Some(kv) = self.kv.get(&k) {
                        let pair = kv.pair();
                        res.push((pair.0.clone(), pair.1.clone()));
                    }
                }
                res
            }
            None => vec![]
        };
        QueryResult {
            query: QueryType::FetchClip(clip.to_string()),
            data: res,
            time_taken: exec.done()
        }
    }

    pub fn fetch_view(&self, view_name: &str) -> QueryResult<Vec<(K, Value)>> {
        let exec = ExecTime::new();
        let res = match self.clips.get_view(view_name) {
            Some(v) => {
                let mut res = Vec::with_capacity(v.value().len());
                for k in v.value().iter() {
                    if let Some(kd) = self.kv.get(&k) {
                        let pair = kd.pair();
                        res.push((pair.0.clone(), pair.1.document().clone()));
                    }
                }
                res
            }
            None => vec![]
        };
        QueryResult {
            query: QueryType::FetchView(view_name.to_string()),
            data: res,
            time_taken: exec.done(),
        }
    }

    pub fn search(&self, query: &str) -> QueryResult<Vec<(K, D)>> {
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
        QueryResult {
            query: QueryType::Lookup,
            data: res,
            time_taken: exec.done()
        }
    }

    pub fn like_search(&self, query: &str) -> QueryResult<ArrayQueue<(K, D)>> {
        let text = query.to_string();
        let exec = ExecTime::new();
        let words: Vec<&str> = text.split_whitespace().collect();
        let keys = self.inverted_idx.w_find(words);
        let res = ArrayQueue::new(keys.len());
        keys.par_iter().for_each(|key|{
            if let Some(kv) = self.kv.get(key) {
                let pair = kv.pair();
                let _ = res.push((pair.0.clone(), pair.1.clone()));
            }
        });
        QueryResult{
            query: QueryType::LikeSearch(text),
            data: res,
            time_taken: exec.done()
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

    pub fn drop(&self) {
        self.hash_idx.clear();
        self.inverted_idx.clear();
        self.range.clear();
        self.clips.clear();
        self.kv.clear();
    }

    pub fn query(&self, cmd: &str) -> Result<QueryResult<SegQueue<(K, D)>>, QueryError> {
        let exec = ExecTime::new();
        let qry = Query::new(cmd);
        if qry.is_err() {
            return Err(qry.err().unwrap());
        }
        let qry = qry.unwrap();
        let res = SegQueue::new();
        self.iter().for_each(|it|{
            let is_ok = qry.filter(it.value());
            if is_ok {
                res.push((it.key().clone(), it.value().clone()));
            }
            if qry.limit.is_some() && res.len() >= qry.limit.unwrap() {
                return ();
            }
        });
        Ok(QueryResult {
            query: QueryType::Query(cmd.to_string()),
            data: res,
            time_taken: exec.done(),
        })
    }
}
