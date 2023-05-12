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
    #[error("error parsing document")]
    DocumentParseError
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
    NoSuchCollection,
    #[error("collection options are not valid")]
    OptionsProvidedAreNotValid
}

#[derive(Serialize, Deserialize, Error, Clone, Debug)]
pub enum QueryError {
    #[error("query parse error `{0}`")]
    ParseError(String),
    #[error("collection `{0}` already exists")]
    CollectionExists(String),
    #[error("collection `{0}` does not exist")]
    CollectionNotExists(String),
    #[error("configuration parse error `{0}`")]
    ConfigureParseError(String),
    #[error("upsert error `{0}`")]
    UpsertError(String),
    #[error("`{0}` supported types: `bool`, `number`, `string`, `null`")]
    CompareError(String),
    #[error("comparison type mismatch")]
    TypeMismatch,
    #[error("direct object or array of objects currently not supported")]
    DirectObjOrArrayOfObj,
    #[error("operator `{0}` not supported for data type `{1}`")]
    OperatorNotAllowed(String,String),
    #[error("unknown operator compare")]
    UnknownOperatorCompare,
    #[error("no result found")]
    NoResult
}
