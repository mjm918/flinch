use std::collections::HashMap;
use anyhow::{Result};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Value, map::Map};
use crate::cmf::CompareFactors;
use crate::doc::Document;
use crate::err::QueryError;
use crate::hdrs::DataTypes;
use crate::kwr::Keywords;

pub struct Query {
    stmt: HashMap<String, Value>,
    pub limit: Option<usize>,
    pub offset: Option<usize>
}

impl Query {
    pub fn new(cmd: &str) -> Result<Self, QueryError> {
        let json: serde_json::error::Result<Value> = serde_json::from_str(cmd);
        if json.is_ok() {
            let json = json.unwrap();
            return if json.is_object() {
                let jobj = json.as_object().unwrap();
                for kv in jobj {
                    if !kv.1.is_object() && !kv.1.is_array() {
                        return Err(QueryError::FilterMustBeArrOrObject(kv.1.to_string()));
                    }
                }

                let mut qry = Query {
                    stmt: HashMap::new(),
                    limit: None,
                    offset: None,
                };

                qry.stmt = HashMap::new();
                for kv in jobj {
                    if kv.0.eq(Keywords::Limit.as_str()) {
                        let lmt = Self::chk_limit(&kv.1);
                        if lmt.is_ok() {
                            qry.limit = Some(lmt.unwrap());
                        } else {
                            return Err(lmt.err().unwrap());
                        }
                    }
                    if kv.0.eq(Keywords::Offset.as_str()) {
                        let offs = Self::chk_offset(&kv.1);
                        if offs.is_ok() {
                            qry.offset = Some(offs.unwrap());
                        } else {
                            return Err(offs.err().unwrap());
                        }
                    }
                    qry.stmt.insert(kv.0.clone(), kv.1.clone());
                }
                Ok(qry)
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
            if key.as_str().eq(Keywords::Or.as_str()) && value.is_array() {
                let arr = value.as_array().unwrap();
                let mut match_count = 0;
                for o in arr.into_iter() {
                    if o.is_object() {
                        let object = o.as_object().unwrap();
                        for (ik,iv) in object {
                            match_count += self.compare_each(&doc, &ik.as_str(), &iv);
                        }
                    }
                }
                if match_count > 0 {
                    is_ok_count += 1;
                }
            } else {
                is_ok_count += self.compare_each(&doc, &key.as_str(), &value);
                filter_count = filter_count + 1;
            }
        }
        is_ok_count >= filter_count && is_ok_count > 0
    }

    fn compare_each(&self, doc: &Map<String,Value>, key: &str, value: &Value) -> i32 {
        let mut is_ok_count = 0;
        if doc.contains_key(key) && value.is_object() {
            let v = doc.get(key).unwrap();
            let fmap = value.as_object().unwrap();
            if v.is_object() {
                let is_ok = self.compare_object(&fmap, &doc);
                if is_ok {
                    is_ok_count += 1;
                }
            }
            if v.is_string() {
                let as_str = v.as_str().unwrap();
                let is_ok = self.compare_string(&fmap, as_str);
                if is_ok {
                    is_ok_count += 1;
                }
            }
            if v.is_array() {
                let as_array = v.as_array().unwrap();
                let is_ok = self.compare_in_array(&fmap, as_array);
                if is_ok {
                    is_ok_count += 1;
                }
            }
            if v.is_f64() {
                let as_f64 = v.as_f64().unwrap();
                let is_ok = self.compare_f64(&fmap, &as_f64);
                if is_ok {
                    is_ok_count += 1;
                }
            }
            if v.is_i64() {
                let as_i64 = v.as_i64().unwrap();
                let is_ok = self.compare_i64(&fmap, &as_i64);
                if is_ok {
                    is_ok_count += 1;
                }
            }
        }
        is_ok_count
    }

    fn factors(&self, fv: &Map<String, Value>) -> CompareFactors {
        let mut cmf = CompareFactors::new();
        let cmds = vec![
            Keywords::Lt.as_str(),
            Keywords::Gt.as_str(),
            Keywords::Eq.as_str(),
            Keywords::Neq.as_str(),
            Keywords::Like.as_str(),
            Keywords::Prop.as_str()
        ];
        for cmd in cmds.into_iter() {
            if fv.contains_key(cmd) {
                cmf.set(cmd, fv.get(cmd).unwrap().clone());
            }
        }
        cmf
    }

    fn compare_i64(&self, fv: &Map<String, Value>, value: &i64) -> bool {
        let comparator = self.factors(fv);
        let mut is_ok = false;
        let str_or_num = comparator.data_type == DataTypes::String || comparator.data_type == DataTypes::Number;
        if str_or_num && comparator.eq.is_some() {
            let fv = fv.get(Keywords::Eq.as_str()).unwrap();
            if fv.is_i64() && value == &fv.as_i64().unwrap() {
                is_ok = true;
            }
        }
        if str_or_num && comparator.neq.is_some() {
            let fv = fv.get(Keywords::Neq.as_str()).unwrap();
            if fv.is_i64() && value != &fv.as_i64().unwrap() {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::Number && comparator.lt.is_some() {
            let fv = fv.get(Keywords::Lt.as_str()).unwrap();
            if fv.is_i64() && value < &fv.as_i64().unwrap() {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::Number && comparator.gt.is_some() {
            let fv = fv.get(Keywords::Gt.as_str()).unwrap();
            if fv.is_i64() && value > &fv.as_i64().unwrap() {
                is_ok = true;
            }
        }
        is_ok
    }

    fn compare_f64(&self, fv: &Map<String, Value>, value: &f64) -> bool {
        let comparator = self.factors(fv);
        let mut is_ok = false;
        let str_or_num = comparator.data_type == DataTypes::String || comparator.data_type == DataTypes::Number;
        if str_or_num && comparator.eq.is_some() {
            let fv = fv.get(Keywords::Eq.as_str()).unwrap();
            if fv.is_f64() && value == &fv.as_f64().unwrap() {
                is_ok = true;
            }
        }
        if str_or_num && comparator.neq.is_some() {
            let fv = fv.get(Keywords::Neq.as_str()).unwrap();
            if fv.is_f64() && value != &fv.as_f64().unwrap() {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::String && comparator.lt.is_some() {
            let fv = fv.get(Keywords::Lt.as_str()).unwrap();
            if fv.is_f64() && value < &fv.as_f64().unwrap() {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::String && comparator.gt.is_some() {
            let fv = fv.get(Keywords::Gt.as_str()).unwrap();
            if fv.is_f64() && value > &fv.as_f64().unwrap() {
                is_ok = true;
            }
        }
        is_ok
    }

    fn compare_in_array(&self, fv: &Map<String, Value>, value: &Vec<Value>) -> bool {
        let mut is_ok = false;
        for obj in value {
            if obj.is_string() && self.compare_string(fv, obj.as_str().unwrap()) {
                is_ok = true;
            }
            if obj.is_i64() && self.compare_i64(fv, &obj.as_i64().unwrap()) {
                is_ok = true;
            }
            if obj.is_f64() && self.compare_f64(fv, &obj.as_f64().unwrap()) {
                is_ok = true;
            }
            if obj.is_object() {
                let mut hmp: Map<String, Value> = Map::new();
                for (key, value) in obj.as_object().unwrap() {
                    hmp.insert(key.clone(), value.clone());
                }
                if self.compare_object(fv, &hmp) {
                    is_ok = true;
                }
            }
        }
        is_ok
    }

    fn compare_string(&self, fv: &Map<String, Value>, value: &str) -> bool {
        let comparator = self.factors(fv);
        let mut is_ok = false;
        if comparator.data_type == DataTypes::String && comparator.eq.is_some() && !value.is_empty()
        {
            let fv = fv.get(Keywords::Eq.as_str()).unwrap();
            if fv.is_string() && value.eq_ignore_ascii_case(fv.as_str().unwrap()) {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::String && comparator.neq.is_some() && !value.is_empty()
        {
            let fv = fv.get(Keywords::Neq.as_str()).unwrap();
            if fv.is_string() && !value.eq_ignore_ascii_case(fv.as_str().unwrap()) {
                is_ok = true;
            }
        }
        if comparator.data_type == DataTypes::String
            && comparator.like.is_some()
            && !value.is_empty()
        {
            let fv = fv.get(Keywords::Like.as_str()).unwrap();
            if fv.is_string() && value.to_string().contains(&fv.as_str().unwrap()) {
                is_ok = true;
            }
        }
        is_ok
    }

    fn compare_object(&self, fv: &Map<String, Value>, value: &Map<String, Value>) -> bool {
        let comparator = self.factors(fv);
        if comparator.prop.is_some() {
            let mut is_ok = false;
            let val: Option<&Value> = value.get(comparator.prop.unwrap().as_str());
            if comparator.data_type == DataTypes::String
                && val.is_some()
                && val.unwrap().is_string()
            {
                is_ok = self.compare_string(fv, &val.unwrap().as_str().unwrap());
            }
            if comparator.data_type == DataTypes::Number
                && val.is_some()
                && val.unwrap().is_number()
            {
                if val.unwrap().is_i64() {
                    is_ok =
                        self.compare_i64(fv, &val.unwrap().as_i64().unwrap());
                }
                if val.unwrap().is_f64() {
                    is_ok =
                        self.compare_f64(fv, &val.unwrap().as_f64().unwrap());
                }
            }
            is_ok
        } else {
            false
        }
    }

    fn chk_limit(value: &Value) -> Result<usize, QueryError> {
        if value.is_i64() {
            let n = value.as_i64().unwrap();
            if n > 0 {
                return Ok(n as usize);
            }
        }
        return Err(QueryError::MustBeNonZero(format!("limit")));
    }

    fn chk_offset(value: &Value) -> Result<usize, QueryError> {
        if value.is_i64() {
            let n = value.as_i64().unwrap();
            if n >= 0 {
                return Ok(n as usize);
            }
        }
        return Err(QueryError::MustBeNonNegative(format!("offset")));
    }
}