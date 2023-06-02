use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc::Sender;
use crate::err::{CollectionError, DocumentError, IndexError, QueryError};

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

/// `ActionType` is used for pubsub
#[derive(Serialize, Deserialize, Clone)]
pub enum NotificationType<K, D> {
    Insert(K, D),
    Remove(K),
}

/// `PubSubEvent` is used for pubsub
#[derive(Clone)]
pub enum PubSubEvent<K, D> {
    Data(NotificationType<K, D>),
    Subscribed(Sender<PubSubEvent<K, D>>),
}

#[derive(Debug)]
pub enum PubSubRes {
    Closed,
    Timeout,
    Err(String),
}

pub enum SortDirection {
    Asc,
    Desc
}

pub struct Sort {
    pub field: String,
    pub direction: SortDirection
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

#[derive(Serialize, Deserialize, Clone)]
pub struct FuncResult<T> {
    pub query: FuncType,
    pub data: T,
    pub time_taken: String
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum FlinchError {
    ExpressionError(String),
    QueryError(QueryError),
    CollectionError(CollectionError),
    DocumentError(DocumentError),
    CustomError(String),
    IndexError(IndexError),
    None
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct QueryResult {
    pub data: Vec<Value>,
    pub error: FlinchError,
    pub time_taken: String
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FlinchCnf {
    pub root: String,
    pub pw: String,
    pub data_dir: String
}
