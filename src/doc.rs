use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::err::{DocumentError, QueryError};
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
    fn new(input: &str) -> Result<Self, DocumentError> where Self: Sized;
    fn set_opts(&mut self, opts: &CollectionOptions);
    fn object(&self) -> &Map<String, Value>;
    fn document(&self) -> &Value;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FromRawString {
    pub data: Value,
    keys: Option<Vec<String>>,
    tokens: Option<Vec<String>>,
    fields: Option<Vec<String>>,
    view_cfg: Vec<ViewConfig>,
    content: Option<Vec<String>>
}

impl Index for FromRawString {
    fn keys(&self) -> Vec<String> {
        if self.keys.is_some() {
            let obj = self.object();
            let mut indexes = vec![];
            for key in self.keys.clone().unwrap() {
                if obj.contains_key(&key) {
                    let v = obj.get(&key)
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string();
                    indexes.push(v);
                }
            }
            indexes
        } else {
            vec![]
        }
    }
}

impl Clips for FromRawString {
    fn tokens(&self) -> Vec<String> {
        if self.tokens.is_some() {
            let obj = self.object();
            let mut tokens = vec![];
            for token in self.tokens.clone().unwrap() {
                if obj.contains_key(&token) {
                    let v = obj.get(&token)
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .to_string();
                    tokens.push(v);
                }
            }
            tokens
        } else {
            vec![]
        }
    }
}

impl Range for FromRawString {
    fn fields(&self) -> Vec<Field> {
        if self.fields.is_some() {
            let obj = self.object();
            let mut rfields = vec![];
            let fields = self.fields.clone().unwrap();
            for field in fields {
                let f = field.as_str();
                if obj.contains_key(f.clone()) {
                    rfields.push(
                        Field{
                                    key: f.clone().to_string(),
                                    value: obj
                                            .get(f.clone())
                                            .unwrap()
                                            .to_string()
                        }
                    );
                }
            }
            rfields
        } else {
            vec![]
        }
    }
}

impl View for FromRawString {
    fn binding(&self) -> Option<String> {
        if self.view_cfg.len() > 0 {
            let obj = self.object();
            let vw_cfg = self.view_cfg.clone();
            for cfg in vw_cfg {
                if obj.contains_key(&cfg.prop) {
                    let v = obj.get(&cfg.prop)
                        .unwrap()
                        .to_string();
                    if v.eq(&cfg.expected) {
                        return Some(cfg.view_name);
                    }
                }
            }
        }
        None
    }
}

impl DocumentSearch for FromRawString {
    fn content(&self) -> Option<String> {
        if self.content.is_some() {
            let obj = self.object();
            let content_fields = self.content.clone().unwrap();
            let mut content = String::from("");
            for content_field in content_fields {
                if obj.contains_key(content_field.as_str()) {
                    let res = obj.get(content_field.as_str());
                    if res.is_some() {
                        let t = res.unwrap().as_str().unwrap().to_string();
                        content.push_str(t.as_str());
                    }
                }
            }
            Some(content)
        } else {
            None
        }
    }
}

impl Document for FromRawString {
    fn new(input: &str) -> Result<Self, DocumentError> {
        let raw = serde_json::from_str::<Value>(input);
        if raw.is_err() {
            return Err(DocumentError::StringParseError);
        }
        let json = raw.unwrap();
        if !json.is_object() {
            return Err(DocumentError::NotAnObject);
        }
        Ok(Self {
            data: json,
            keys: None,
            tokens: None,
            fields: None,
            view_cfg: vec![],
            content: None,
        })
    }

    fn set_opts(&mut self, opts: &CollectionOptions) {
        let k = self.keys().len() > 0;
        let t = self.tokens().len() > 0;
        let f = self.fields().len() > 0;
        let fl = self.binding().is_some();
        let c = self.content().is_some();

        self.keys = if !k { Some(opts.index_opts.clone()) } else { None };
        self.tokens = if !t { Some(opts.clips_opts.clone()) } else { None };
        self.view_cfg = if !f { opts.view_opts.clone() } else { vec![] };
        self.fields = if !fl { Some(opts.range_opts.clone()) } else { None };
        self.content = if !c { Some(opts.search_opts.clone()) } else { None };
    }

    fn object(&self) -> &Map<String, Value> {
        self.data.as_object().unwrap()
    }

    fn document(&self) -> &Value {
        &self.data
    }
}
