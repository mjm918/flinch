use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Serialize, Deserialize, Error, Clone, Debug)]
pub enum IndexError {
    #[error("document already exists in index")]
    DuplicateDocument,
    #[error("no such index")]
    NoSuchIndex,
}

#[derive(Serialize, Deserialize, Error, Clone, Debug)]
pub enum DocumentError {
    #[error("string cannot be converted to document")]
    StringParseError,
    #[error("invalid document provided. must be a key-value pair")]
    NotAnObject,
}

#[derive(Serialize, Deserialize, Error, Clone, Debug)]
pub enum WatcherError {
    #[error("sender not found")]
    SenderNotFound,
    #[error("sender already exists")]
    SendersRepetitive,
}

#[derive(Serialize, Deserialize, Error, Clone, Debug)]
pub enum CollectionError {
    #[error("collection already exists")]
    DuplicateCollection,
    #[error("no such collection")]
    NoSuchCollection
}

#[derive(Serialize, Deserialize, Error, Clone, Debug)]
pub enum QueryError {
    #[error("query expression error `{0}`")]
    ParseError(String),
    #[error("query is not valid object")]
    QueryIsNotObject,
    #[error("filter must be array or object. found `{0}`")]
    FilterMustBeArrOrObject(String),
    #[error("no result found")]
    NoResult,
    #[error("value of `{0}` must be non-negative")]
    MustBeNonNegative(String),
    #[error("value of `{0}` must be non-zero or non-negative")]
    MustBeNonZero(String),
    #[error("invalid sort operator")]
    InvalidSort
}
