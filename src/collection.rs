use std::sync::Arc;

use anyhow::Result;
use crossbeam_queue::ArrayQueue;
use dashmap::{DashMap, DashSet};
use dashmap::rayon::map::Iter;
use log::{debug, trace};
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use sled::Db;

use crate::clips::Clips;
use crate::database::CollectionOptions;
use crate::doc_trait::Document;
use crate::errors::IndexError;
use crate::events::EVENT_EMITTER;
use crate::headers::{FuncResult, FuncType, NotificationType, PubSubEvent, PubSubRes};
use crate::index_fields::InvertedIndex;
use crate::index_hash::HashIndex;
use crate::persistent::Persistent;
use crate::pub_sub::PubSub;
use crate::range::Range;
use crate::ttl::{Entry, Ttl};
use crate::utils::{ExecTime, get_ttl_name, prefix_doc, prefix_ttl, TTL_PREFIX, uuid};
use crate::watchman::Watchman;

pub type ExecutionTime = String;
pub type K = String;

/// Collection is a document storage
pub struct Collection<D: Document> {
    ttl: Arc<Ttl>,
    bkp: Persistent,
    kv: DashMap<K, D>,
    hash_idx: HashIndex<K>,
    inverted_idx: InvertedIndex<K>,
    clips: Clips<K>,
    range: Range<K>,
    watchman: PubSub<PubSubEvent<K, D>>,
    pub opts: CollectionOptions,
}

impl<D> Collection<D>
    where
        D: Serialize + DeserializeOwned + Clone + Send + Sync + 'static + Document
{
    /// create a new collection based on `sled` instance and `CollectionOptions`.
    /// boots previously inserted data
    /// runs TTL engine
    /// watch TTL events
    pub async fn new(db: &Db, opts: CollectionOptions) -> Arc<Self> {
        let option = opts.clone();
        let ttl = Arc::new(Ttl::new(option.name.as_str()));
        let instance = Arc::new(Self {
            ttl,
            bkp: Persistent::open(&db, option.name.as_str()),
            kv: DashMap::new(),
            hash_idx: HashIndex::new(),
            inverted_idx: InvertedIndex::new(),
            clips: Clips::new(),
            range: Range::new(),
            watchman: Watchman::<PubSubEvent<K, D>>::new(vec![]).unwrap().start(),
            opts,
        });
        instance.boot().await;

        let runtime = tokio::runtime::Handle::current();
        let this = Arc::clone(&instance);
        EVENT_EMITTER.lock().unwrap().on("expired", move |entry: Entry| {
            runtime.block_on(async {
                trace!("expired key - {:?}",&entry);
                this._delete(entry.key, true).await;
            });
        });
        instance
    }

    async fn boot(&self) {
        let res = self.bkp.fetch_doc();
        debug!("loading {} records from local storage in collection {}",res.len(), &self.opts.name);
        for kv in res {
            let _ = self._put(kv.0, kv.1, false).await;
        }
        let bkp_ttl = self.bkp.prefix(TTL_PREFIX.to_string());
        debug!("{} items queued for expiry",bkp_ttl.len());
        for kv in bkp_ttl {
            let key = get_ttl_name(kv.0.as_str());
            let value = kv.1.parse::<i64>();
            if value.is_ok() {
                let ttl = value.unwrap();
                self.ttl.push(ttl, key);
            } else {
                debug!("error parsing timestamp {} {:?}",kv.1,value.err().unwrap());
            }
        }
        debug!("booting TTL engine");
        let ttl = Arc::clone(&self.ttl);
        std::thread::spawn(move || {
            ttl.start();
        });
    }

    /// `pubsub` subscriber for Insert or Delete event
    pub async fn sub(&self, sx: tokio::sync::mpsc::Sender<PubSubEvent<K, D>>) -> Result<(), PubSubRes> {
        debug!("subscriber added to watchman");
        let _ = self.watchman.notify(PubSubEvent::Subscribed(sx.clone())).await;
        self.watchman.reg(sx).await
    }

    /// sets a TTL for a `Pointer`
    pub async fn put_ttl(&self, k: K, timestamp: i64) {
        self.bkp.put_any(prefix_ttl(k.as_str()), timestamp);
        self.ttl.push(timestamp, k);
    }

    /// creates a document in the collection. `K` is type of `String` and represents a `Pointer`
    #[inline]
    pub async fn put(&self, k: K, d: D) -> Result<ExecutionTime, IndexError> {
        self._put(k, d, true).await
    }

    #[inline]
    async fn _put(&self, k: K, d: D, new: bool) -> Result<ExecutionTime, IndexError> {
        let exec = ExecTime::new();
        let mut v = d;
        v.set_opts(&self.opts);

        if !self.opts.index_opts.is_empty() {
            // FIXME: need to find a better way to handle this. if same index is found, apply upsert logic
            if let Err((_err, key)) = self.hash_idx.put(&k, &v) {
                if let Some((pointer, _value)) = self.get_index(key.as_str()).data {
                    self.delete(pointer).await;
                    let _ = self.hash_idx.put(&k, &v);
                }
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
        if new {
            self.bkp.put(prefix_doc(k.as_str()), v.clone());
        }

        let query = NotificationType::Insert(k.to_string(), v);
        let _ = self.watchman.notify(PubSubEvent::Data(query)).await;

        Ok(exec.done())
    }

    /// deletes a document by a `Pointer`
    #[inline]
    pub async fn delete(&self, k: K) -> ExecutionTime {
        self._delete(k, true).await
    }

    #[inline]
    pub async fn _delete(&self, k: K, rm_ttl: bool) -> ExecutionTime {
        let exec = ExecTime::new();
        if self.kv.contains_key(&k) {
            let query = NotificationType::<K, D>::Remove(k.clone());
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
            self.range.delete(&k, &v);

            let _ = self.bkp.remove(prefix_doc(k.as_str()));
        }
        if rm_ttl {
            self.ttl.remove(k.to_string());
            let _ = self.bkp.remove(prefix_ttl(k.as_str()));
        }
        trace!("deleted {}. remaining items left {}",k, self.len());
        exec.done()
    }

    /// deletes document based on `Range`
    #[inline]
    pub async fn delete_by_range(&self, field: &str, from: String, to: String) -> ExecutionTime {
        let exec = ExecTime::new();
        let res = self.fetch_range(field, from, to).data;
        for kv in res.iter() {
            self.delete(kv.0.clone()).await;
        }
        exec.done()
    }

    /// deletes clip
    #[inline]
    pub async fn delete_by_clip(&self, clip: &str) -> ExecutionTime {
        let exec = ExecTime::new();
        let res = self.fetch_clip(clip).data;
        for kv in res.iter() {
            self.delete(kv.0.clone()).await;
        }
        exec.done()
    }

    /// gets documents based on array of `Pointers`
    #[inline]
    pub fn multi_get(&self, keys: Vec<&K>) -> FuncResult<Vec<(K, D)>> {
        let exec = ExecTime::new();
        let mut res = Vec::with_capacity(keys.len());
        keys.iter().for_each(|k| {
            if let Some(v) = self.kv.get(k.clone()) {
                let pair = v.pair();
                res.push((pair.0.clone(), pair.1.clone()));
            }
        });
        FuncResult {
            query: FuncType::LookupMulti,
            data: res,
            time_taken: exec.done(),
        }
    }

    /// gets documents based on `Range` filter
    #[inline]
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

    /// gets a specific document by `Pointer`
    #[inline]
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
            time_taken: exec.done(),
        }
    }

    /// gets a document by `index` value
    #[inline]
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
            time_taken: exec.done(),
        }
    }

    /// gets a clip by `name`
    #[inline]
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
            time_taken: exec.done(),
        }
    }

    /// fetch a view by `name`
    #[inline]
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

    /// Search like type as you go
    #[inline]
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
            time_taken: exec.done(),
        }
    }

    /// search in inverted index
    #[inline]
    pub fn like_search(&self, query: &str) -> FuncResult<ArrayQueue<(K, D)>> {
        let text = query.to_string();
        let exec = ExecTime::new();
        let words: Vec<&str> = text.split_whitespace().collect();
        let keys = self.inverted_idx.w_find(words);
        if keys.len() > 0 {
            let res = ArrayQueue::new(keys.len());
            keys.par_iter().for_each(|key| {
                if let Some(kv) = self.kv.get(key) {
                    let pair = kv.pair();
                    let _ = res.push((pair.0.clone(), pair.1.clone()));
                }
            });
            FuncResult {
                query: FuncType::LikeSearch(text),
                data: res,
                time_taken: exec.done(),
            }
        } else {
            FuncResult {
                query: FuncType::LikeSearch(text),
                data: ArrayQueue::new(1),
                time_taken: exec.done(),
            }
        }
    }

    /// returns an parallel iterator on `Flinch` storage
    #[inline]
    pub fn iter(&self) -> Iter<'_, K, D> {
        self.kv.par_iter()
    }

    /// returns an parallel iterator on `Flinch` hash index
    pub fn iter_index(&self) -> Iter<'_, String, K> {
        self.hash_idx.iter()
    }

    /// returns an parallel iterator on `Flinch` clip storage
    pub fn iter_clips(&self) -> Iter<String, DashSet<K>> {
        self.clips.iter()
    }

    /// returns total number of documents available in the storage
    #[inline]
    pub fn len(&self) -> usize {
        self.kv.len()
    }

    /// generates an UUID. can use it as a pointer
    #[inline]
    pub fn id(&self) -> String {
        uuid()
    }

    /// truncate current collection
    pub async fn empty(&self) {
        let local = self.kv.clone();
        for kv in local {
            self.delete(kv.0).await;
        }
        self.clips.clear_keys();
        self.range.clear_trees();
    }
    /// allows to save documents in `sled` storage on demand
    #[inline]
    pub async fn flush_bkp(&self) -> usize {
        self.bkp.flush().await
    }
}
