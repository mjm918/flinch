use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::err::{DocumentError};
use crate::db::CollectionOptions;

#[derive(Clone, Serialize, Deserialize)]
pub struct Field {
    pub key: String,
    pub value: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ViewConfig {
    pub prop: String,
    pub expected: String,
    pub view_name: String
}

pub trait Index {
    fn keys(&self) -> Vec<String>;
}

pub trait Clips {
    fn tokens(&self) -> Vec<String>;
}

pub trait Range {
    fn fields(&self) -> Vec<Field>;
}

pub trait View {
    fn binding(&self) -> Option<String>;
}

pub trait DocumentSearch {
    fn content(&self) -> Option<String>;
}

pub trait Document: Index + Clips + Range + View + DocumentSearch {
    fn from_str(input:&str) -> Result<Self, DocumentError> where Self: Sized;
    fn from_value(input: &Value) -> Result<Self, DocumentError> where Self: Sized;
    fn set_opts(&mut self, opts: &CollectionOptions);
    fn object(&self) -> &Map<String, Value>;
    fn document(&self) -> &Value;
    fn string(&self) -> String;
}

