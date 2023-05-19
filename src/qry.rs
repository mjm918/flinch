use std::time::Instant;
use crossbeam_queue::SegQueue;
use rayon::prelude::*;
use evalexpr::eval_with_context;
use flql::Flql;
use futures::executor::{block_on};
use futures::StreamExt;
use serde_json::{Number, Value};
use crate::col::Collection;
use crate::db::{CollectionOptions, Database};
use crate::doc::Document;
use crate::docv::QueryBased;
use crate::err::{CollectionError, DocumentError, IndexError, QueryError};
use crate::hdrs::{ActionResult, FlinchError, FuncResult};
use crate::utils::trim_col_name;

pub struct Query {
    db: Database<String, QueryBased>
}

impl Query {
    pub fn new() -> Self {
        Self { db: Database::<String, QueryBased>::init() }
    }

    pub async fn exec(&self, stmt: &str) -> ActionResult {
        let parsed = flql::parse(stmt);
        if parsed.is_err() {
            return ActionResult{
                data: vec![],
                error: FlinchError::CustomError(parsed.err().unwrap()),
                time_taken: "".to_string(),
            };
        }
        let parsed = parsed.unwrap();
        match parsed {
            Flql::New(options) => {
                let ttk = Instant::now();
                let x = self.new_c(options);
                ActionResult {
                    data: vec![],
                    error: self.err_q(x.err()),
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::Drop(collection) => {
                let ttk = Instant::now();
                let x = self.db.drop_c(collection.as_str()).await;
                ActionResult {
                    data: vec![],
                    error: self.err_c(x.err()),
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::Exists(pointer,collection) => {
                let ttk = Instant::now();
                let col = self.db.using(collection.as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                
                let exi = col.get(&pointer);
                let res = if exi.data.is_some() {
                    let data = exi.data.unwrap();
                    vec![data.1.data]
                } else {
                    vec![]
                };
                ActionResult{
                    data: res,
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::Length(collection) => {
                let ttk = Instant::now();
                let col = self.db.using(collection.as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                
                ActionResult{
                    data: vec![Value::Number(Number::from(col.len()))],
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::Put(data, collection) => {
                let ttk = Instant::now();
                let qdata = QueryBased::from_str(data.as_str());
                if qdata.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_d(qdata.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = self.db.using(trim_col_name(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();

                let id = col.id();
                let x = col.put(id.clone(), qdata.unwrap()).await;
                ActionResult{
                    data: vec![Value::String(id)],
                    error: self.err_i(x.err()),
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::PutWhen(data, condition, collection) => {
                let ttk = Instant::now();
                let qdata = QueryBased::from_str(data.as_str());
                if qdata.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_d(qdata.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = self.db.using(collection.as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let qdata = qdata.unwrap();
                let col = col.unwrap();
                
                let sg = SegQueue::new();
                col.iter().for_each(|kv|{
                    let pair = kv.pair();
                    let v = pair.1.document();
                    let k = pair.0;
                    let ctx = eval_with_context(condition.as_str(), &Self::data_context_filter(&v));
                    if ctx.is_ok() {
                        let x = block_on(async {
                            col.put(k.clone(), qdata.clone()).await
                        });
                        if x.is_ok() {
                            sg.push(Value::String(k.clone()));
                        }
                    }
                });
                let ids = sg.into_iter().collect();
                ActionResult{
                    data: ids,
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::PutPointer(data, pointer, collection) => {
                let ttk = Instant::now();
                let qdata = QueryBased::from_str(data.as_str());
                if qdata.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_d(qdata.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = self.db.using(collection.as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let qdata = qdata.unwrap();
                let col = col.unwrap();
                let x = col.put(pointer, qdata).await;
                let mut time_taken = format!("{:?}",ttk.elapsed());
                if x.is_ok() {
                    time_taken = x.unwrap();
                }
                ActionResult{
                    data: vec![],
                    error: FlinchError::None,
                    time_taken,
                }
            }
            Flql::Get(collection) => {
                let ttk = Instant::now();
                let sg = SegQueue::new();
                let col = self.db.using(collection.as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                
                col.iter().for_each(|kv|{
                    let pair = kv.pair();
                    sg.push(pair.1.document().clone());
                });
                ActionResult{
                    data: sg.into_iter().collect(),
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::GetWhen(condition, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(collection.as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                
                let sg = SegQueue::new();
                col.iter().for_each(|kv|{
                    let pair = kv.pair();
                    let v = pair.1.document();
                    let ctx = eval_with_context(condition.as_str(), &Self::data_context_filter(&v));
                    if ctx.is_ok() {
                        sg.push(v.clone());
                    }
                });
                ActionResult{
                    data: sg.into_iter().collect(),
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::GetPointer(pointer, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(collection.as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                
                let v = col.get(&pointer);

                let mut time_taken = format!("{:?}",ttk.elapsed());
                let mut res = vec![];
                if v.data.is_some() {
                    time_taken = v.time_taken;

                    let d = v.data.unwrap();
                    res = vec![d.1.data];
                }
                ActionResult{
                    data: res,
                    error: FlinchError::None,
                    time_taken,
                }
            }
            Flql::GetView(view, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(collection.as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                
                let v = col.fetch_view(&view);

                let mut time_taken = format!("{:?}",ttk.elapsed());
                let mut res = vec![];
                if v.data.len() > 0 {
                    time_taken = v.time_taken;

                    let sg = SegQueue::new();
                    v.data.par_iter().for_each(|tuple|{
                        sg.push(tuple.1.clone());
                    });
                    res = sg.into_iter().collect();
                }
                ActionResult{
                    data: res,
                    error: FlinchError::None,
                    time_taken,
                }
            }
            Flql::GetClip(clip, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(collection.as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                
                let c = col.fetch_clip(clip.as_str());

                let time_taken = format!("{:?}",ttk.elapsed());

                let data = c.data;
                let sg = SegQueue::new();
                data.par_iter().for_each(|kv|{
                   sg.push(kv.1.data.clone());
                });
                let res = sg.into_iter().collect();
                ActionResult{
                    data: res,
                    error: FlinchError::None,
                    time_taken,
                }
            }
            Flql::Delete(collection) => {
                ActionResult{
                    data: vec![],
                    error: FlinchError::None,
                    time_taken: "".to_string(),
                }
            }
            Flql::DeleteWhen(condition, collection) => {
                ActionResult{
                    data: vec![],
                    error: FlinchError::None,
                    time_taken: "".to_string(),
                }
            }
            Flql::DeletePointer(pointer, collection) => {
                ActionResult{
                    data: vec![],
                    error: FlinchError::None,
                    time_taken: "".to_string(),
                }
            }
            Flql::DeleteView(view, collection) => {
                ActionResult{
                    data: vec![],
                    error: FlinchError::None,
                    time_taken: "".to_string(),
                }
            }
            Flql::DeleteClip(clip, collection) => {
                ActionResult{
                    data: vec![],
                    error: FlinchError::None,
                    time_taken: "".to_string(),
                }
            }
            Flql::None => {
                ActionResult{
                    data: vec![],
                    error: FlinchError::None,
                    time_taken: "".to_string(),
                }
            }
        }
    }

    fn new_c(&self, options: String) ->Result<(), QueryError> {
        let parsed:serde_json::Result<CollectionOptions> = serde_json::from_str(options.as_str());
        if parsed.is_ok() {
            let opts = parsed.unwrap();
            let x = self.db.add(opts);
            if x.is_err() {
                return Err(QueryError::CollectionError(x.err().unwrap()));
            }
        } else {
            return Err(QueryError::ConfigureParseError(parsed.err().unwrap().to_string()));
        }
        Ok(())
    }

    fn err_c(&self, error: Option<CollectionError>) -> FlinchError {
        if error.is_some() {
            FlinchError::CollectionError(error.unwrap())
        } else {
            FlinchError::None
        }
    }

    fn err_q(&self, error: Option<QueryError>) -> FlinchError {
        if error.is_some() {
            FlinchError::QueryError(error.unwrap())
        } else {
            FlinchError::None
        }
    }

    fn err_d(&self, error: Option<DocumentError>) -> FlinchError {
        if error.is_some() {
            FlinchError::DocumentError(error.unwrap())
        } else {
            FlinchError::None
        }
    }

    fn err_i(&self, error: Option<IndexError>) -> FlinchError {
        if error.is_some() {
            FlinchError::IndexError(error.unwrap())
        } else {
            FlinchError::None
        }
    }
}