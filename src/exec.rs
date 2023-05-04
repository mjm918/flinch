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
        for dql in parsed {
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
                    sg.push(res);
                },
                Dql::Drop(name) => {
                    async {
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
                        sg.push(res);
                    };
                },
                Dql::Len(name) =>{
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
                    sg.push(res);
                },
                Dql::Upsert(name, doc, clause) =>{
                    async {
                        let _ = self.upsert(name.as_str(), &doc, clause).await;
                    };
                },
                Dql::UpsertWithoutClause(name, doc) =>{
                    async {
                        let _ = self.upsert(name.as_str(),&doc,None).await;
                    };
                },
                Dql::Put(name, index, doc) =>{},
                Dql::Exists(name, index) =>{},
                Dql::Search(name, query, sort, limit) =>{},
                Dql::GetIndex(name, index) =>{},
                Dql::GetWithoutClause(name, sort, limit) =>{},
                Dql::Get(name, clause, sort, limit) =>{},
                Dql::DeleteIndex(name, index) =>{},
                Dql::DeleteWithoutClause(name) =>{},
                Dql::Delete(name, clause) =>{},
                Dql::None => {}
            };
        }
        sg
    }

    fn create_c(&self, name: &str, option: Value) -> Result<(), QueryError> {
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

    async fn upsert(&self, name: &str, doc: &Value, clause: Option<Clause>) -> Result<ExecutionTime, QueryError> {
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

    fn length_c(&self, name:&str) -> Result<Value, QueryError> {
        let exi = self.col(name.clone());
        if exi.is_ok() {
            let col = exi.unwrap();
            return Ok(Value::Number(Number::from(col.value().len())));
        }
        Err(QueryError::CollectionNotExists(name.to_string()))
    }

    fn filtered_keys(&self, col: Ref<String, Collection<String, QueryBased>>, clause: &Option<Clause>) -> SegQueue<String> {
        let v = SegQueue::new();
        let clause = clause.as_ref().unwrap();
        if clause.and.is_some() {
            let mut all_of = false;
            let and = clause.and.as_ref().unwrap();
            let and = and.as_array().unwrap();

            col.value().iter().for_each(|item|{
                let doc = item.value().object();

            });
        }
        if clause.or.is_some() {
            let or = clause.or.as_ref().unwrap();
            let or = or.as_array().unwrap();
            let mut either_one = false;
            for cnd in or {
                let condition = cnd.as_object().unwrap();

            }
        }
        v
    }

    fn cmp(&self, doc: &Map<String, Value>, cond: &Vec<Value>) -> bool {
        let mut found = false;
        for cnd in cond {
            let condition = cnd.as_object().unwrap();
            for operator in condition {
                let field_cmp = operator.1.as_object().unwrap();

                let mut key= vec![];

                let mut key_v: Value = Value::Null;
                let mut cmp_v: Value = Value::Null;

                for kv in field_cmp {
                    key = kv.0.split(".").collect::<Vec<&str>>();
                    cmp_v = kv.1.clone();
                }
                match operator.0.as_str() {
                    "$eq" => {

                    }
                    "$neq" => {}
                    "$inc" => {}
                    "$ninc" => {}
                    "$lt" => {}
                    "$lte" => {}
                    "$gt" => {}
                    "$gte" => {}
                    "$like" => {}
                    _=>{}
                }
            }
        }
        found
    }

    fn recur_v(&self, mut props: Vec<&str>, doc: &Map<String, Value>, current: Option<&Value>) -> Value {
        let mut idx = 0;
        for key in &props {
            let k = key.clone();
            if current.is_some() {
                let nd = current.unwrap();
                if nd.is_object() {
                    let nd = nd.as_object().unwrap();
                    if nd.contains_key(k) {
                        let mut nv = vec![];
                        while idx != 0 {
                            nv.push(props.remove(idx));
                            idx -= 1;
                        }
                        return self.recur_v(nv, doc, doc.get(k));
                    }
                }
            } else {
                if doc.contains_key(k) {
                    let mut nv = vec![];
                    while idx != 0 {
                        nv.push(props.remove(idx));
                        idx -= 1;
                    }
                    return self.recur_v(nv, doc, doc.get(k));
                }
            }
            idx += 1;
        }
        current.unwrap().clone()
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