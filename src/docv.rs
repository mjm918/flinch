use serde_json::{Map, Value};
use serde::{Deserialize, Serialize};
use crate::db::CollectionOptions;
use crate::doc::{Clips, Document, DocumentSearch, Field, Index, Range, View, ViewConfig};
use crate::err::DocumentError;

#[derive(Clone, Serialize, Deserialize)]
pub struct QueryBased {
    pub data: Value,
    keys: Option<Vec<String>>,
    tokens: Option<Vec<String>>,
    fields: Option<Vec<String>>,
    view_cfg: Vec<ViewConfig>,
    content: Option<Vec<String>>
}

impl Index for QueryBased {
    fn keys(&self) -> Vec<String> {
        if self.keys.is_some() {
            let obj = self.object();
            let mut indexes = vec![];
            for key in self.keys.clone().unwrap() {
                if obj.contains_key(&key) {
                    let chk = obj.get(&key)
                        .unwrap()
                        .as_str();
                    if chk.is_some() {
                        let v = chk.unwrap()
                            .to_string();
                        indexes.push(v);
                    }
                }
            }
            indexes
        } else {
            vec![]
        }
    }
}

impl Clips for QueryBased {
    fn tokens(&self) -> Vec<String> {
        if self.tokens.is_some() {
            let obj = self.object();
            let mut tokens = vec![];
            for token in self.tokens.clone().unwrap() {
                if obj.contains_key(&token) {
                    let v = obj.get(&token)
                        .unwrap();
                    if v.as_str().is_some() {
                        let chk = v
                            .as_str();
                        if chk.is_some() {
                            let v = chk.unwrap()
                                .to_string();
                            tokens.push(v);
                        }
                    }
                }
            }
            tokens
        } else {
            vec![]
        }
    }
}

impl Range for QueryBased {
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

impl View for QueryBased {
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

impl DocumentSearch for QueryBased {
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

impl Document for QueryBased {

    fn from_str(input: &str) -> anyhow::Result<Self, DocumentError> where Self: Sized {
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

    fn from_value(input: &Value) -> anyhow::Result<Self, DocumentError> where Self: Sized {
        if !input.is_object() {
            return Err(DocumentError::NotAnObject);
        }
        Ok(Self {
            data: input.clone(),
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

    fn make(&self, key: String) -> Value {
        let mut obj = self.object().to_owned();
        obj.insert("_pointer".to_owned(), Value::String(key));
        Value::Object(obj)
    }

    fn string(&self) -> String {
        serde_json::to_string(self.document()).unwrap()
    }
}