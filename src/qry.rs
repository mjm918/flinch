use anyhow::{Result};
use dashmap::DashMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Value, map::Map};
use crate::doc::Document;
use crate::err::QueryError;

pub struct Query {
    cmd: DashMap<String, Map<String, Value>>
}

impl Query {
    pub fn new(cmd: &str) -> Result<Self, QueryError> {
        let json: serde_json::error::Result<Value> = serde_json::from_str(cmd);
        if json.is_ok() {
            let json = json.unwrap();
            return if json.is_object() {
                let mut cmd = DashMap::new();
                let jobj = json.as_object().unwrap();
                for kv in jobj {
                    if !kv.1.is_object() {
                        return Err(QueryError::FilterMustBeObject);
                    }
                    if cmd.contains_key(kv.0.as_str()) {
                        cmd.remove(kv.0.as_str());
                    }
                    cmd.insert(kv.0.clone(), kv.1.as_object().unwrap().clone());
                }
                Ok(Self { cmd })
            } else {
                Err(QueryError::QueryIsNotObject)
            }
        }
        Err(QueryError::ParseError(json.err().unwrap().to_string()))
    }

    pub fn filter<D>(&self, value: D)
        where D: Serialize + DeserializeOwned + Clone + Send + Sync + Document + 'static
    {
        let obj = value.document();

    }
}