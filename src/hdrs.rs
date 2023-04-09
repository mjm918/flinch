use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

pub struct ExecTime {
    pub timer: Instant,
}

impl ExecTime {
    pub fn new() -> Self {
        Self {
            timer: Instant::now()
        }
    }
    pub fn done(&self) -> String {
        format!("{:?}", self.timer.elapsed())
    }
}

#[allow(dead_code)]
pub enum WatcherState {
    Continue,
    Disconnected,
    Empty,
}

pub struct DestinationDown<M>(M);
pub enum Request<M> {
    Register(Sender<M>),
    Dispatch(M)
}

#[derive(Serialize, Deserialize, Clone)]
pub enum Query<K, D> {
    Insert(K, D),
    Remove(K),
}
#[derive(Clone)]
pub enum Event<K, D> {
    Query(Query<K, D>),
    Subscribed(Sender<Event<K, D>>),
}
#[derive(Debug)]
pub enum DbRes {
    End,
    Err(String),
    Duplicate,
}
#[derive(Debug)]
pub enum SessionRes {
    Closed,
    Timeout,
    Full,
    NoResponse,
    DataStoreNotFound,
    UnImplement,
    Err(String),
}

pub static TIMEOUT: Duration = Duration::from_secs(5);

pub fn set_view_name(name: &str) -> String {
    format!(":view:{}",name)
}
pub fn get_view_name(name: &str) -> String {
    name.replace(":view:","")
}