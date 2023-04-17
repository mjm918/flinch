use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

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
pub enum ActionType<K, D> {
    Insert(K, D),
    Remove(K),
}
#[derive(Clone)]
pub enum Event<K, D> {
    Data(ActionType<K, D>),
    Subscribed(Sender<Event<K, D>>),
}

#[derive(Debug)]
pub enum SessionRes {
    Closed,
    Timeout,
    Err(String),
}
