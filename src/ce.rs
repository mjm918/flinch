/// git: https://github.com/whitfin/retainer.git
use std::marker::PhantomData;
use std::ops::{Deref, Range};
use std::time::{Duration, Instant};

use rand::prelude::*;

#[derive(Debug)]
pub(crate) struct CacheEntry<V> {
    value: V,
    expiration: CacheExpiration,
}

impl<V> CacheEntry<V> {
    pub fn new(value: V, expiration: CacheExpiration) -> Self {
        Self { value, expiration }
    }

    pub fn expiration(&self) -> &CacheExpiration {
        &self.expiration
    }

    pub fn value(&self) -> &V {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut V {
        &mut self.value
    }

    pub fn into_inner(self) -> V {
        self.value
    }
}

#[derive(Debug)]
pub struct CacheExpiration {
    instant: Option<Instant>,
}

impl CacheExpiration {
    
    pub fn new<I>(instant: I) -> Self
        where
            I: Into<Instant>,
    {
        Self {
            instant: Some(instant.into()),
        }
    }

    pub fn none() -> Self {
        Self { instant: None }
    }

    pub fn instant(&self) -> &Option<Instant> {
        &self.instant
    }

    pub fn is_expired(&self) -> bool {
        self.instant()
            .map(|expiration| expiration < Instant::now())
            .unwrap_or(false)
    }

    pub fn remaining(&self) -> Option<Duration> {
        self.instant
            .map(|i| i.saturating_duration_since(Instant::now()))
    }
}

impl From<Instant> for CacheExpiration {
    fn from(instant: Instant) -> Self {
        Self::new(instant)
    }
}

impl From<u64> for CacheExpiration {
    fn from(millis: u64) -> Self {
        Duration::from_millis(millis).into()
    }
}

impl From<Duration> for CacheExpiration {
    fn from(duration: Duration) -> Self {
        Instant::now().checked_add(duration).unwrap().into()
    }
}

impl From<Range<u64>> for CacheExpiration {
    fn from(range: Range<u64>) -> Self {
        rand::thread_rng().gen_range(range).into()
    }
}

#[derive(Debug)]
pub struct CacheReadGuard<'a, V> {
    pub(crate) entry: *const CacheEntry<V>,
    pub(crate) marker: PhantomData<&'a CacheEntry<V>>,
}

impl<'a, V> CacheReadGuard<'a, V> {
    /// Retrieve the internal guarded expiration.
    pub fn expiration(&self) -> &CacheExpiration {
        self.entry().expiration()
    }

    pub fn value(&self) -> &V {
        self.entry().value()
    }

    fn entry(&self) -> &CacheEntry<V> {
        unsafe { &*self.entry }
    }
}

impl<'a, V> Deref for CacheReadGuard<'a, V> {
    type Target = V;
    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

unsafe impl<V> Send for CacheReadGuard<'_, V> where V: Sized + Sync {}
unsafe impl<V> Sync for CacheReadGuard<'_, V> where V: Sized + Send + Sync {}