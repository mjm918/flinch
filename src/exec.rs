use std::time::Instant;
use anyhow::{Result};
use crossbeam_queue::SegQueue;
use dashmap::mapref::one::Ref;
use rayon::prelude::*;
use dql::{Clause, Dql};
use serde_json::{Map, Number, Value};
use crate::col::{Collection, ExecutionTime};
use crate::db::{CollectionOptions, Database};
use crate::doc::Document;
use crate::docv::QueryBased;
use crate::err::QueryError;
use crate::hdrs::QueryResult;

pub struct Query {
    db: Database<String, QueryBased>
}

impl Query {
    pub fn new(db: Database<String, QueryBased>) -> Self {
        Self { db }
    }

    pub fn exec(&self, ql: &str) -> SegQueue<QueryResult> {
        let ttk = Instant::now();
        let sg = SegQueue::<QueryResult>::new();

        let parsed = dql::parse(ql);
        if parsed.is_err() {
            sg.push(
                QueryResult{
                    resp: Value::Null,
                    error: format!("{}", parsed.err().unwrap()),
                    time_taken: "".to_string(),
                }
            );
            return sg;
        }

        let parsed = parsed.unwrap();
        parsed.par_iter().for_each(|dql|{
            let x = futures::executor::block_on(async {
                match dql {
                    Dql::Create(name, option) =>{
                        let mut res = QueryResult {
                            resp: Value::Null,
                            error: "".to_string(),
                            time_taken: "".to_string(),
                        };
                        let x = self.create_c( name.as_str(), option);
                        res.resp = Value::Bool(x.is_ok());
                        if !x.is_ok() {
                            res.error = x.err().unwrap().to_string();
                        }
                        res.time_taken = format!("{:?}",ttk.elapsed());
                        res
                    },
                    Dql::Drop(name) => {
                        let mut res = QueryResult {
                            resp: Value::Null,
                            error: "".to_string(),
                            time_taken: "".to_string(),
                        };
                        let x = self.drop_c(name.as_str()).await;
                        res.resp = Value::Bool(x.is_ok());
                        if x.is_err() {
                            res.error = x.err().unwrap().to_string();
                        }
                        res.time_taken = format!("{:?}",ttk.elapsed());
                        res
                    },
                    Dql::Len(name) => {
                        let mut res = QueryResult {
                            resp: Value::Null,
                            error: "".to_string(),
                            time_taken: "".to_string(),
                        };
                        let x = self.length_c(name.as_str());
                        if x.is_ok() {
                            res.resp = x.unwrap();
                        }
                        res.time_taken = format!("{:?}",ttk.elapsed());
                        res
                    },
                    Dql::Upsert(name, doc, clause) => {
                        let mut res = QueryResult {
                            resp: Value::Null,
                            error: "".to_string(),
                            time_taken: "".to_string(),
                        };
                        let x = self.upsert(name.as_str(), &doc, clause).await;
                        if x.is_ok() {
                            res.time_taken = x.unwrap();
                        } else {
                            res.error = x.err().unwrap().to_string();
                        }
                        res
                    },
                    Dql::UpsertWithoutClause(name, doc) => {
                        let mut res = QueryResult {
                            resp: Value::Null,
                            error: "".to_string(),
                            time_taken: "".to_string(),
                        };
                        let x = self.upsert(name.as_str(),&doc,&None).await;
                        if x.is_ok() {
                            res.time_taken = x.unwrap();
                        } else {
                            res.error = x.err().unwrap().to_string();
                        }
                        res
                    },
                    /*Dql::Put(name, index, doc) =>{},
                    Dql::Exists(name, index) =>{},
                    Dql::Search(name, query, sort, limit) =>{},
                    Dql::GetIndex(name, index) =>{},
                    Dql::GetWithoutClause(name, sort, limit) =>{},
                    Dql::Get(name, clause, sort, limit) =>{},
                    Dql::DeleteIndex(name, index) =>{},
                    Dql::DeleteWithoutClause(name) =>{},
                    Dql::Delete(name, clause) =>{},
                    Dql::None => {},*/
                    _=>{
                        QueryResult{
                            resp: Default::default(),
                            error: "".to_string(),
                            time_taken: "".to_string(),
                        }
                    }
                }
            });
            sg.push(x);
        });
        sg
    }

    fn create_c(&self, name: &str, option: &Value) -> Result<(), QueryError> {
        let opt: serde_json::error::Result<CollectionOptions> = serde_json::from_value(option.clone());
        if opt.is_err() {
            return Err(QueryError::ConfigureParseError(option.to_string()));
        }
        let mut opt = opt.unwrap();
        opt.name = Some(format!("{}",name));
        let x = self.db.add(opt);
        if x.is_err() {
            return Err(QueryError::CollectionExists(name.to_string()));
        }
        Ok(())
    }

    async fn drop_c(&self, name: &str) -> Result<(), QueryError> {
        let x = self.db.drop(name).await;
        if x.is_err() {
            return Err(QueryError::CollectionNotExists(name.to_string()));
        }
        Ok(())
    }

    fn length_c(&self, name:&str) -> Result<Value, QueryError> {
        let exi = self.col(name.clone());
        if exi.is_ok() {
            let col = exi.unwrap();
            return Ok(Value::Number(Number::from(col.value().len())));
        }
        Err(QueryError::CollectionNotExists(name.to_string()))
    }

    async fn upsert(&self, name: &str, doc: &Value, clause: &Option<Clause>) -> Result<ExecutionTime, QueryError> {
        let exi = self.col(&name);
        if exi.is_ok() {
            let col = exi.unwrap();
            let opts = &col.value().opts;
            let d = QueryBased::from_value(doc);
            if d.is_err() {
                return Err(QueryError::ParseError(format!("failed to parse document value")));
            }
            let mut document = d.unwrap();
                document.set_opts(opts.clone());

            let ex = col.put(col.id(),document).await;
            if ex.is_ok() {
                return Ok(ex.unwrap());
            }
            return Err(QueryError::UpsertError(ex.err().unwrap().to_string()));
        }
        Err(QueryError::CollectionNotExists(name.to_string()))
    }

    fn filtered_keys(&self, col: Ref<String, Collection<String, QueryBased>>, clause: &Option<Clause>) -> SegQueue<String> {
        let v = SegQueue::new();
        let clause = clause.as_ref().unwrap();
        if clause.and.is_some() {
            let mut matched_all = false;
            let and = clause.and.as_ref().unwrap();
            let and = and.as_array().unwrap();

            col.value().iter().for_each(|item|{
                let doc = item.value().document();

            });
        }
        if clause.or.is_some() {
            let mut either_one = false;
            let or = clause.or.as_ref().unwrap();
            let or = or.as_array().unwrap();

        }
        v
    }

    fn cmp(&self, doc: &Value, cond: &Vec<Value>) -> Result<(), QueryError> {
        for cnd in cond {
            let condition = cnd.as_object().unwrap();
            for operator in condition {
                let field_cmp = operator.1.as_object().unwrap();

                let mut key= format!("");
                let mut cmp_v: Value = Value::Null;

                for kv in field_cmp {
                    key = format!("/{}",kv.0.replace(".","/"));
                    cmp_v = kv.1.clone();
                }
                let mut key_v: Value = self.find_from_doc(doc,key.as_str());
                if !self.cmp_type_ok(&key_v) {
                    return Err(QueryError::CompareError(format!("{}",operator.0)));
                }
                if !self.cmp_type_ok(&cmp_v) {
                    return Err(QueryError::CompareError(format!("{}",&cmp_v.to_string())));
                }
                if !self.type_cmp(&key_v, &cmp_v) {
                    return Err(QueryError::TypeMismatch);
                }

            }
        }
        Ok(())
    }

    fn both_cmp(&self, from_doc: &Value, cmp: &Value, operator: &str) -> Result<(), QueryError> {
        let string = vec!["$eq","$neq","$lte","$gte","$gt","$lt","$like"];
        let number = vec!["$eq","$neq","$lte","$gte","$gt","$lt"];
        let boolean = vec!["$eq"];
        let array = vec!["$inc","$ninc"];
        if let Some(rhs) = from_doc.as_str() {
            if !string.contains(&operator) {
                return Err(QueryError::OperatorNotAllowed(format!("{}",&operator),format!("String")));
            }
        }
        if let Some(rhs) = from_doc.as_f64() {
            if !number.contains(&operator) {
                return Err(QueryError::OperatorNotAllowed(format!("{}",&operator),format!("Number")));
            }
        }
        if let Some(rhs) = from_doc.as_null() {
            if !boolean.contains(&operator) {
                return Err(QueryError::OperatorNotAllowed(format!("{}",&operator),format!("Null")));
            }
        }
        if let Some(rhs) = from_doc.as_bool() {
            if !boolean.contains(&operator) {
                return Err(QueryError::OperatorNotAllowed(format!("{}",&operator),format!("Boolean")));
            }
        }
        if let Some(rhs) = from_doc.as_array() {

        }
        if let Some(rhs) = from_doc.as_object() {
            return Err(QueryError::DirectObjOrArrayOfObj);
        }
        Ok(())
    }

    fn type_cmp(&self, right: &Value, left: &Value) -> bool {
        (right.is_string() && left.is_string()) ||
            (right.is_number() && left.is_number()) ||
            (right.is_null() && left.is_null()) ||
            (right.is_boolean() && left.is_boolean())
    }

    fn cmp_type_ok(&self, v: &Value) -> bool {
        v.is_string() || v.is_null() || v.is_number() || v.is_boolean()
    }

    fn find_from_doc(&self, doc: &Value, keys: &str) -> Value {
        let pointer = doc.pointer(keys);
        if pointer.is_some() {
            return pointer.unwrap().clone();
        }
        Value::Null
    }

    fn col(&self, name:&str) -> Result<Ref<String, Collection<String, QueryBased>>, QueryError> {
        let col = self.db.using(name.clone());
        if col.is_ok() {
            Ok(col.unwrap())
        } else {
            Err(QueryError::CollectionNotExists(name.to_string()))
        }
    }
}