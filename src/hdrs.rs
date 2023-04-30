use anyhow::Error;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
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
pub enum PubSubEvent<K, D> {
    Data(ActionType<K, D>),
    Subscribed(Sender<PubSubEvent<K, D>>),
}

#[derive(Debug)]
pub enum PubSubRes {
    Closed,
    Timeout,
    Err(String),
}

#[derive(Serialize, Deserialize, Clone)]
pub enum FuncType {
    Lookup,
    LookupMulti,
    LookupIndex(String),
    LikeSearch(String),
    FetchView(String),
    FetchClip(String),
    FetchRange(String),
    Query(String)
}

#[derive(PartialEq)]
pub enum DataTypes {
    String,
    Number
}

#[derive(Serialize, Deserialize, Clone)]
pub struct FuncResult<T> {
    pub query: FuncType,
    pub data: T,
    pub time_taken: String
}

#[derive(Serialize, Deserialize, Clone)]
pub struct QueryResult {
    pub query: String,
    pub data: Vec<Value>,
    pub errors: Vec<String>,
    pub time_taken: String
}

