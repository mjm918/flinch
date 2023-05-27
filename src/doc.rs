use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::err::{DocumentError};
use crate::db::CollectionOptions;

/// `Field` is used in `CollectionOptions`
/// for range configuration
/// Consider using it for string props. Because at the end, `Field` columns will be
/// converted to string. Use it for `Range` filter.
#[derive(Clone, Serialize, Deserialize)]
pub struct Field {
    pub key: String,
    pub value: String,
}
/// `ViewConfig` is used for `View` filter
#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct ViewConfig {
    pub prop: String,
    pub expected: String,
    pub view_name: String
}
/// `Index` is similar to RDMS unique key
pub trait Index {
    fn keys(&self) -> Vec<String>;
}
/// `Clips` help to group documents
pub trait Clips {
    fn tokens(&self) -> Vec<String>;
}
/// `Range` is for filtering a range of values
pub trait Range {
    fn fields(&self) -> Vec<Field>;
}
/// `View` is similar to `Tag`
/// For example: Get all documents that matches
/// prop `age` expected `18` and `view_name` is ADULT
pub trait View {
    fn binding(&self) -> Option<String>;
}
/// `DocumentSearch` is used for search engine
pub trait DocumentSearch {
    fn content(&self) -> Option<String>;
}

pub trait Document: Index + Clips + Range + View + DocumentSearch {
    fn from_str(input:&str) -> Result<Self, DocumentError> where Self: Sized;
    fn from_value(input: &Value) -> Result<Self, DocumentError> where Self: Sized;
    fn set_opts(&mut self, opts: &CollectionOptions);
    fn object(&self) -> &Map<String, Value>;
    fn document(&self) -> &Value;
    fn make(&self, key: String) -> Value;
    fn string(&self) -> String;
}

