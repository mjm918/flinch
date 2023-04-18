use anyhow::{Result};
use std::iter::Map;
use serde_json::{Value};
use crate::err::QueryError;

pub struct Query<'a> {
    cmd: Map<&'a String, &'a Value>
}

impl<'a> Query<'a> {
    /*pub fn new(cmd: &str) -> Result<Self, QueryError> {
        let json = serde_json::from_str(cmd);
        if json.is_ok() {
            return Ok(Self {
                cmd: json.unwrap()
            });
        }
        Err(QueryError::ParseError(json.err().unwrap().to_string()))
    }*/

}