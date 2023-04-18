use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use anyhow::{Result};
use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Value, map::Map};
use crate::doc::Document;
use crate::err::QueryError;

pub struct Query {
    stmt: HashMap<String, Value>
}

#[derive(PartialEq)]
enum DataTypes {
    String,
    Number,
}
struct CompareFactors {
    lt: Option<bool>,
    gt: Option<bool>,
    eq: Option<bool>,
    like: Option<bool>,
    prop: Option<String>,
    data_type: DataTypes,
}

const LT: &str = "$lt";
const GT: &str = "$gt";
const EQ: &str = "$eq";
const LIKE: &str = "$like";
const PROP: &str = "$prop";

impl Query {
    pub fn new(cmd: &str) -> Result<Self, QueryError> {
        let json: serde_json::error::Result<Value> = serde_json::from_str(cmd);
        if json.is_ok() {
            let json = json.unwrap();
            return if json.is_object() {
                let mut cmd = Map::new();
                let jobj = json.as_object().unwrap();
                for kv in jobj {
                    if !kv.1.is_object() {
                        return Err(QueryError::FilterMustBeObject);
                    }
                    if cmd.contains_key(kv.0.as_str()) {
                        cmd.remove(kv.0.as_str());
                    }
                    cmd.insert(kv.0.clone(), kv.1.clone());
                }
                let mut stmt = HashMap::new();
                for kv in cmd {
                    stmt.insert(kv.0, kv.1);
                }
                Ok(Self { stmt })
            } else {
                Err(QueryError::QueryIsNotObject)
            }
        }
        Err(QueryError::ParseError(json.err().unwrap().to_string()))
    }

    pub fn filter<D>(&self, row: &D) -> bool
        where D: Serialize + DeserializeOwned + Clone + Send + Sync + Document + 'static
    {
        let doc = row.object();
        let mut is_ok_count = 0;
        let mut filter_count = 0;
        for (key, value) in &self.stmt {
            let mut is_ok = false;
            if doc.contains_key(key.as_str()) && value.is_object() {
                let dval = doc.get(key.as_str()).unwrap();
                let fmap = value.as_object().unwrap();
                if dval.is_object() {
                    is_ok = self.compare_by_filter_obj(&fmap, &doc);
                    if is_ok {
                        is_ok_count = is_ok_count + 1;
                    }
                }
                if dval.is_string() {
                    let as_str = dval.as_str().unwrap();
                    is_ok = self.compare_by_filter_str(&fmap, as_str);
                    if is_ok {
                        is_ok_count = is_ok_count + 1;
                    }
                }
                if dval.is_array() {
                    let as_array = dval.as_array().unwrap();
                    is_ok = self.compare_by_filter_array(&fmap, as_array);
                    if is_ok {
                        is_ok_count = is_ok_count + 1;
                    }
                }
                if dval.is_f64() {
                    let as_f64 = dval.as_f64().unwrap();
                    is_ok = self.compare_by_filter_f64(&fmap, &as_f64);
                    if is_ok {
                        is_ok_count = is_ok_count + 1;
                    }
                }
                if dval.is_i64() {
                    let as_i64 = dval.as_i64().unwrap();
                    is_ok = self.compare_by_filter_i64(&fmap, &as_i64);
                    if is_ok {
                        is_ok_count = is_ok_count + 1;
                    }
                }
            }
            filter_count = filter_count + 1;
        }
        filter_count == is_ok_count
    }

    fn factors(&self, fv: &Map<String, Value>) -> CompareFactors {
        let mut data_type = DataTypes::String;
        let mut cf = CompareFactors {
            lt: if fv.contains_key(LT) {
                Some(fv.contains_key(LT))
            } else {
                None
            },
            gt: if fv.contains_key(GT) {
                Some(fv.contains_key(GT))
            } else {
                None
            },
            eq: if fv.contains_key(EQ) {
                Some(fv.contains_key(EQ))
            } else {
                None
            },
            like: if fv.contains_key(LIKE) {
                Some(fv.contains_key(LIKE))
            } else {
                None
            },
            prop: if fv.contains_key(PROP) {
                Some(fv.get(PROP).unwrap().to_string())
            } else {
                None
            },
            data_type: DataTypes::String,
        };
        if cf.lt.is_some() {
            let v = fv.get(LT).unwrap();
            data_type = if v.is_f64() || v.is_i64() {
                DataTypes::Number
            } else {
                DataTypes::String
            };
        }
        if cf.gt.is_some() {
            let v = fv.get(GT).unwrap();
            data_type = if v.is_f64() || v.is_i64() {
                DataTypes::Number
            } else {
                DataTypes::String
            };
        }
        if cf.eq.is_some() {
            let v = fv.get(EQ).unwrap();
            data_type = if v.is_f64() || v.is_i64() {
                DataTypes::Number
            } else {
                DataTypes::String
            };
        }
        if cf.like.is_some() {
            let v = fv.get(LIKE).unwrap();
            data_type = if v.is_f64() || v.is_i64() {
                DataTypes::Number
            } else {
                DataTypes::String
            };
        }
        cf.data_type = data_type;
        cf
    }

    fn compare_by_filter_i64(&self, fv: &Map<String, Value>, value: &i64) -> bool {
        let comparator = self.factors(fv);
        let mut is_ok = false;
        if (comparator.data_type == DataTypes::String || comparator.data_type == DataTypes::Number)
            && comparator.eq.is_some()
        {
            let fv = fv.get(EQ).unwrap();
            if fv.is_i64() && value == &fv.as_i64().unwrap() {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::Number && comparator.lt.is_some() {
            let fv = fv.get(LT).unwrap();
            if fv.is_i64() && value < &fv.as_i64().unwrap() {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::Number && comparator.gt.is_some() {
            let fv = fv.get(GT).unwrap();
            if fv.is_i64() && value > &fv.as_i64().unwrap() {
                is_ok = true;
            }
        }
        is_ok
    }

    fn compare_by_filter_f64(&self, fv: &Map<String, Value>, value: &f64) -> bool {
        let comparator = self.factors(fv);
        let mut is_ok = false;
        if comparator.data_type == DataTypes::String && comparator.eq.is_some() {
            let fv = fv.get(EQ).unwrap();
            if fv.is_f64() && value == &fv.as_f64().unwrap() {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::String && comparator.lt.is_some() {
            let fv = fv.get(LT).unwrap();
            if fv.is_f64() && value < &fv.as_f64().unwrap() {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::String && comparator.gt.is_some() {
            let fv = fv.get(GT).unwrap();
            if fv.is_f64() && value > &fv.as_f64().unwrap() {
                is_ok = true;
            }
        }
        is_ok
    }

    fn compare_by_filter_array(&self, fv: &Map<String, Value>, value: &Vec<Value>) -> bool {
        let mut is_ok = false;
        for obj in value {
            if obj.is_string() && self.compare_by_filter_str(fv, obj.as_str().unwrap()) {
                is_ok = true;
            }
            if obj.is_i64() && self.compare_by_filter_i64(fv, &obj.as_i64().unwrap()) {
                is_ok = true;
            }
            if obj.is_f64() && self.compare_by_filter_f64(fv, &obj.as_f64().unwrap()) {
                is_ok = true;
            }
            if obj.is_object() {
                let mut hmp: Map<String, Value> = Map::new();
                for (key, value) in obj.as_object().unwrap() {
                    hmp.insert(key.clone(), value.clone());
                }
                if self.compare_by_filter_obj(fv, &hmp) {
                    is_ok = true;
                }
            }
        }
        is_ok
    }

    fn compare_by_filter_str(&self, fv: &Map<String, Value>, value: &str) -> bool {
        let comparator = self.factors(fv);
        let mut is_ok = false;
        if comparator.data_type == DataTypes::String && comparator.eq.is_some() && !value.is_empty()
        {
            let fv = fv.get(EQ).unwrap();
            if fv.is_string() && value.eq_ignore_ascii_case(fv.as_str().unwrap()) {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::String
            && comparator.like.is_some()
            && !value.is_empty()
        {
            let fv = fv.get(LIKE).unwrap();
            if fv.is_string() && value.to_string().contains(&fv.as_str().unwrap()) {
                is_ok = true;
            }
        }
        is_ok
    }

    fn compare_by_filter_obj(&self, fv: &Map<String, Value>, value: &Map<String, Value>) -> bool {
        let comparator = self.factors(fv);
        if comparator.prop.is_some() {
            let mut is_ok = false;
            let val: Option<&Value> = value.get(comparator.prop.unwrap().as_str());
            if comparator.data_type == DataTypes::String
                && val.is_some()
                && val.unwrap().is_string()
            {
                is_ok = self.compare_by_filter_str(fv, &val.unwrap().as_str().unwrap());
            }
            if comparator.data_type == DataTypes::Number
                && val.is_some()
                && val.unwrap().is_number()
            {
                if val.unwrap().is_i64() {
                    is_ok =
                        self.compare_by_filter_i64(fv, &val.unwrap().as_i64().unwrap());
                }
                if val.unwrap().is_f64() {
                    is_ok =
                        self.compare_by_filter_f64(fv, &val.unwrap().as_f64().unwrap());
                }
            }
            is_ok
        } else {
            false
        }
    }
}