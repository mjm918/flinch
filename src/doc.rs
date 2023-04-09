use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::err::DocumentError;
use crate::flinch::StoreOptions;

#[derive(Clone, Serialize, Deserialize)]
pub struct Field {
    pub key: String,
    pub value: String,
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
    fn filter(&self) -> Option<String>;
}

pub trait DocumentSearch {
    fn content(&self) -> Option<String>;
}

pub trait Document: Index + Clips + Range + View + DocumentSearch {
    fn new(input: &str) -> Result<Self, DocumentError> where Self: Sized;
    fn set_opts(&mut self, opts: &StoreOptions);
}

#[derive(Clone, Serialize, Deserialize)]
pub struct FromRawString {
    pub raw: HashMap<String, Value>,
    pub keys: Option<Vec<String>>,
    pub tokens: Option<Vec<String>>,
    pub fields: Option<Vec<Field>>,
    pub filter: Option<String>,
    pub content: Option<Vec<String>>
}

impl Index for FromRawString {
    fn keys(&self) -> Vec<String> {
        if self.keys.is_some() {
            self.keys.clone().unwrap()
        } else {
            vec![]
        }
    }
}

impl Clips for FromRawString {
    fn tokens(&self) -> Vec<String> {
        if self.tokens.is_some() {
            self.tokens.clone().unwrap()
        } else {
            vec![]
        }
    }
}

impl Range for FromRawString {
    fn fields(&self) -> Vec<Field> {
        if self.fields.is_some() {
            self.fields.clone().unwrap()
        } else {
            vec![]
        }
    }
}

impl View for FromRawString {
    fn filter(&self) -> Option<String> {
        if self.filter.is_some() {
            Some(self.filter.clone().unwrap())
        } else {
            None
        }
    }
}

impl DocumentSearch for FromRawString {
    fn content(&self) -> Option<String> {
        if self.content.is_some() {
            let content_fields = self.content.clone().unwrap();
            let mut content = String::from("");
            for content_field in content_fields {
                let res = self.raw.get(content_field.as_str());
                if res.is_some() {
                    let t = res.unwrap().as_str().unwrap().to_string();
                    content.push_str(t.as_str());
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
        let mut raw_json = HashMap::new();
        let map = json.as_object().unwrap();
        for kv in map {
            raw_json.insert(kv.0.clone(), kv.1.clone());
        }
        Ok(Self {
            raw: raw_json,
            keys: None,
            tokens: None,
            fields: None,
            filter: None,
            content: None,
        })
    }

    fn set_opts(&mut self, opts: &StoreOptions) {
        let k = self.keys().len() > 0;
        let t = self.tokens().len() > 0;
        let f = self.fields().len() > 0;
        let fl = self.filter().is_some();
        let c = self.content().is_some();

        self.keys = if !k { Some(opts.index_opts.clone()) } else { None };
        self.tokens = if !t { Some(opts.clips_opts.clone()) } else { None };
        self.filter = if !f { opts.view_opts.clone() } else { None };
        self.fields = if !fl { Some(opts.range_opts.clone()) } else { None };
        self.content = if !c { Some(opts.search_opts.clone()) } else { None };
    }
}
