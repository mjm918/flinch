use std::cmp;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};
use std::marker::PhantomData;
use std::time::{Duration, Instant};

use async_lock::{RwLock, RwLockUpgradableReadGuard};
use async_timer::Interval;
use log::{debug, log_enabled, trace, Level};
use rand::prelude::*;
use tokio::sync::mpsc::{UnboundedSender as Sender};
use crate::ce::{CacheEntry, CacheExpiration, CacheReadGuard};

macro_rules! unpack {
    ($entry: expr) => {
        if $entry.expiration().is_expired() {
            None
        } else {
            Some($entry)
        }
    };
}

pub struct Cache<K, V> {
    store: RwLock<BTreeMap<K, CacheEntry<V>>>,
    label: String,
    sender: Option<Sender<K>>
}

impl<K, V> Cache<K, V>
    where
        K: Ord + Clone + Display + Debug,
{
    pub fn new(sender: Option<Sender<K>>) -> Self {
        Self {
            store: RwLock::new(BTreeMap::new()),
            label: "".to_owned(),
            sender,
        }
    }

    pub fn with_label(mut self, s: &str) -> Self {
        self.label = format!("cache({}): ", s);
        self
    }

    pub async fn clear(&self) {
        self.store.write().await.clear()
    }

    pub async fn expired(&self) -> usize {
        self.store
            .read()
            .await
            .iter()
            .filter(|(_, entry)| entry.expiration().is_expired())
            .count()
    }

    pub async fn get(&self, k: &K) -> Option<CacheReadGuard<'_, V>> {
        let guard = self.store.read().await;
        let found = guard.get(k)?;
        let valid = unpack!(found)?;

        Some(CacheReadGuard {
            entry: valid,
            marker: PhantomData,
        })
    }

    pub async fn len(&self) -> usize {
        self.store.read().await.len()
    }

    pub async fn insert<E>(&self, k: K, v: V, e: E) -> Option<V>
        where
            E: Into<CacheExpiration>,
    {
        let entry = CacheEntry::new(v, e.into());
        self.store
            .write()
            .await
            .insert(k, entry)
            .and_then(|entry| unpack!(entry))
            .map(CacheEntry::into_inner)
    }

    pub async fn is_empty(&self) -> bool {
        self.store.read().await.is_empty()
    }

    pub async fn monitor(&self, sample: usize, threshold: f64, frequency: Duration) {
        let mut interval = Interval::platform_new(frequency);
        loop {
            interval.as_mut().await;
            self.purge(sample, threshold).await;
        }
    }

    pub async fn purge(&self, sample: usize, threshold: f64) {
        let start = Instant::now();

        let mut locked = Duration::from_nanos(0);
        let mut removed = 0;

        loop {
            let store = self.store.upgradable_read().await;

            if store.is_empty() {
                break;
            }

            let total = store.len();
            let sample = cmp::min(sample, total);

            let mut gone = 0;
            let mut keys = Vec::with_capacity(sample);
            let mut indices: BTreeSet<usize> = BTreeSet::new();

            {
                let mut rng = rand::thread_rng();
                while indices.len() < sample {
                    indices.insert(rng.gen_range(0..total));
                }
            }

            {
                let mut prev = 0;
                let mut iter: Box<dyn Iterator<Item = (&K, &CacheEntry<V>)>> =
                    Box::new(store.iter());

                for idx in indices {
                    let offset = idx
                        .checked_sub(prev)
                        .and_then(|idx| idx.checked_sub(1))
                        .unwrap_or(0);

                    iter = Box::new(iter.skip(offset));
                    prev = idx;

                    let (key, entry) = iter.next().unwrap();

                    if !entry.expiration().is_expired() {
                        continue;
                    }

                    keys.push(key.to_owned());

                    gone += 1;
                }
            }

            {
                let acquired = Instant::now();
                let mut store = RwLockUpgradableReadGuard::upgrade(store).await;

                for key in &keys {
                    if let Some(sender) = &self.sender {
                        sender.send(key.clone()).unwrap();
                    }
                    store.remove(key);
                }

                locked = locked.checked_add(acquired.elapsed()).unwrap();
            }

            if log_enabled!(Level::Trace) {
                trace!(
                    "{}removed {} / {} ({:.2}%) of the sampled keys",
                    self.label,
                    gone,
                    sample,
                    (gone as f64 / sample as f64) * 100f64,
                );
            }

            removed += gone;

            if (gone as f64) < (sample as f64 * threshold) {
                break;
            }
        }

        if log_enabled!(Level::Debug) {
            debug!(
                "{}purge loop removed {} entries in {:.0?} ({:.0?} locked)",
                self.label,
                removed,
                start.elapsed(),
                locked
            );
        }
    }

    pub async fn remove(&self, k: &K) -> Option<V> {
        self.store
            .write()
            .await
            .remove(k)
            .and_then(|entry| unpack!(entry))
            .map(CacheEntry::into_inner)
    }

    pub async fn unexpired(&self) -> usize {
        self.store
            .read()
            .await
            .iter()
            .filter(|(_, entry)| !entry.expiration().is_expired())
            .count()
    }

    pub async fn update<F>(&self, k: &K, f: F)
        where
            F: FnOnce(&mut V),
    {
        let mut guard = self.store.write().await;
        if let Some(entry) = guard.get_mut(k).and_then(|entry| unpack!(entry)) {
            f(entry.value_mut());
        }
    }
}

impl<K, V> Default for Cache<K, V>
    where
        K: Ord + Clone + Display + Debug,
{
    fn default() -> Self {
        Cache::new(None)
    }
}