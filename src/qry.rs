use std::cmp::Ordering;
use std::time::Instant;
use crossbeam_queue::SegQueue;
use rayon::prelude::*;
use flql::Flql;
use futures::executor::{block_on};
use log::{debug, trace};
use serde_json::{Number, Value};
use tokio::sync::mpsc::Sender;
use crate::db::{CollectionOptions, Database};
use crate::doc::Document;
use crate::docv::QueryBased;
use crate::err::{CollectionError, DocumentError, IndexError, QueryError};
use crate::hdrs::{ActionResult, FlinchError, PubSubEvent, Sort, SortDirection};
use crate::utils::{parse_limit, parse_sort, trim_apos};

/// creates a `Query` session for
/// executing `flql`
pub struct Query {
    db: Database<QueryBased>,
    current: String
}

impl Query {
    /// creates an instance of `Flinch` database
    pub async fn new() -> Self {
        let db = Database::<QueryBased>::init().await;
        Self { db, current: "".to_string() }
    }

    /// `pubsub` for new documents or remove document event
    pub async fn subscribe(&self, name:&str, sx: Sender<PubSubEvent<String, QueryBased>>) -> Result<(), FlinchError> {
        let col = self.db.using(name);
        if col.is_err() {
            return Err(FlinchError::CollectionError(CollectionError::NoSuchCollection));
        }
        let col = col.unwrap();
        Ok(col.sub(sx).await.unwrap())
    }

    /// expect an argument `flql` statement
    pub async fn exec(&mut self, stmt: &str) -> ActionResult {
        trace!("flql executing {}", &stmt.chars().take(60).collect::<String>());

        self.current = format!("{}",&stmt);

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
                let x = self.new_c(options).await;
                ActionResult {
                    data: vec![],
                    error: self.err_q(x.err()),
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::Drop(collection) => {
                let ttk = Instant::now();
                let x = self.db.drop(collection.as_str()).await;
                ActionResult {
                    data: vec![],
                    error: self.err_c(x.err()),
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::Flush(collection) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                col.flush_bkp().await;
                ActionResult {
                    data: vec![],
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::Exists(pointer,collection) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let ttk = Instant::now();
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
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let ttk = Instant::now();
                ActionResult{
                    data: vec![Value::Number(Number::from(col.len()))],
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::Ttl(duration,condition,collection) => {
                let ttk = Instant::now();
                let timestamp = duration.parse::<i64>();
                if timestamp.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_s(format!("{} is malformed as a TTL value",duration)),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let expression = flql::expr_parse(trim_apos(&condition).as_str());
                if expression.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_s(expression.err().unwrap().to_string()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let expression = expression.unwrap();
                let timestamp = chrono::Local::now() + chrono::Duration::seconds(timestamp.unwrap());
                let timestamp = timestamp.timestamp();
                let ttk = Instant::now();
                let data = col.iter().filter(|kv|{
                    let pair = kv.pair();
                    let d = pair.1;
                    let x = expression.calculate(d.string().as_bytes());
                    if x.is_ok() {
                        let x = x.unwrap();
                        if x == flql::exp_parser::Value::Bool(true) {
                            return true;
                        }
                    }
                    false
                })
                    .map(|kv|{
                        let key = kv.key();
                        key.to_string()
                    })
                    .collect::<Vec<String>>();
                // FIXME: do in par_iter
                for key in &data {
                    col.put_ttl(key.to_string(), timestamp).await;
                }
                let message = format!("TTL was set for {} keys", data.len());
                let data = vec![Value::String(message)];
                ActionResult{
                    data,
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
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let ttk = Instant::now();
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
                let expression = flql::expr_parse(trim_apos(&condition).as_str());
                if expression.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_s(expression.err().unwrap().to_string()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let qdata = QueryBased::from_str(data.as_str());
                if qdata.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_d(qdata.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let expression = expression.unwrap();
                let qdata = qdata.unwrap();
                let col = col.unwrap();
                let ttk = Instant::now();
                let sg = SegQueue::new();
                col.iter().for_each(|kv|{
                    let pair = kv.pair();
                    let v = pair.1;
                    let k = pair.0;
                    let d = expression.calculate(v.string().as_bytes());
                    if d.is_ok() {
                        let d = d.unwrap();
                        if d == flql::exp_parser::Value::Bool(true) {
                            let x = block_on(async {
                                col.put(k.clone(), qdata.clone()).await
                            });
                            if x.is_ok() {
                                sg.push(Value::String(k.clone()));
                            }
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
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let qdata = qdata.unwrap();
                let col = col.unwrap();
                let ttk = Instant::now();
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
            Flql::SearchTyping(query, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let ttk = Instant::now();
                let res = col.search(trim_apos(&query).as_str());
                let time_taken = format!("{:?}",ttk.elapsed());
                let data = res.data.into_iter().map(|kv|kv.1.document().clone()).collect::<Vec<Value>>();
                ActionResult{
                    data,
                    error: FlinchError::None,
                    time_taken,
                }
            }
            Flql::Get(collection,sort,limit) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let sort = parse_sort(sort);
                let limit = parse_limit(limit);

                let col = col.unwrap();
                let ttk = Instant::now();
                let mut data = col.iter().map(|kv|{
                    let k = kv.key();
                    let v = kv.value();
                    v.make(k.clone())
                }).collect::<Vec<Value>>();

                self.proc(
                    &mut data,
                    sort
                );

                ActionResult{
                    data: if let Some((offset, limit)) = limit {
                        data[offset..(offset + limit)].to_owned()
                    } else {
                        data
                    },
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::GetWhen(condition, collection,sort,limit) => {
                let ttk = Instant::now();
                let expression = flql::expr_parse(trim_apos(&condition).as_str());
                if expression.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_s(expression.err().unwrap().to_string()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let expression = expression.unwrap();
                let sort = parse_sort(sort);
                let limit = parse_limit(limit);

                let col = col.unwrap();
                let ttk = Instant::now();
                let mut data = col.iter().filter(|kv|{
                    let pair = kv.pair();
                    let d = pair.1;
                    let x = expression.calculate(d.string().as_bytes());
                    if x.is_ok() {
                        let x = x.unwrap();
                        if x == flql::exp_parser::Value::Bool(true) {
                            return true;
                        }
                    }
                    false
                })
                    .map(|kv|{
                        let key = kv.key();
                        let doc = kv.value();
                        doc.make(key.to_owned())
                    })
                    .collect::<Vec<Value>>();
                self.proc(
                    &mut data,
                    sort
                );
                ActionResult{
                    data: if let Some((offset, limit)) = limit {
                        data[offset..(offset + limit)].to_owned()
                    } else {
                        data
                    },
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::GetPointer(pointer, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
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
                    res = vec![d.1.make(d.0)];
                }
                ActionResult{
                    data: res,
                    error: FlinchError::None,
                    time_taken,
                }
            }
            Flql::GetView(view, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
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
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let ttk = Instant::now();
                let c = col.fetch_clip(clip.as_str());
                let time_taken = format!("{:?}",ttk.elapsed());

                let data = c.data;
                let sg = SegQueue::new();
                data.par_iter().for_each(|kv|{
                   sg.push(kv.1.make(kv.0.to_owned()));
                });
                let res = sg.into_iter().collect();
                ActionResult{
                    data: res,
                    error: FlinchError::None,
                    time_taken,
                }
            }
            Flql::GetIndex(index, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let ttk = Instant::now();
                let x = col.get_index(trim_apos(&index).as_str());
                let mut data = vec![];
                if x.data.is_some() {
                    let y = x.data.unwrap();
                    data.push(y.1.make(y.0));
                }
                ActionResult{
                    data,
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::GetRange(start, end, on, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let ttk = Instant::now();
                let res = col.fetch_range(trim_apos(&on).as_str(), trim_apos(&start), trim_apos(&end));
                let data = res.data.iter().map(|kv|kv.1.make(kv.0.to_owned())).collect::<Vec<Value>>();
                ActionResult{
                    data,
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::Delete(collection) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let ttk = Instant::now();
                let _ = col.empty().await;
                ActionResult{
                    data: vec![],
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed())
                }
            }
            Flql::DeleteWhen(condition, collection) => {
                let ttk = Instant::now();
                let expression = flql::expr_parse(trim_apos(&condition).as_str());
                if expression.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_s(expression.err().unwrap().to_string()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let expression = expression.unwrap();
                let col = col.unwrap();
                let sg = SegQueue::new();
                let ttk = Instant::now();
                col.iter().for_each(|kv|{
                    let pair = kv.pair();
                    let v = pair.1;
                    let d = expression.calculate(v.string().as_bytes());
                    if d.is_ok() {
                        let d = d.unwrap();
                        if d == flql::exp_parser::Value::Bool(true) {
                            let k = pair.0;
                            let _ = block_on(async{
                                col.delete(k.to_string()).await;
                                sg.push(k.to_string());
                            });
                        }
                    }
                });
                let res = sg.into_iter().map(|s|Value::String(s)).collect::<Vec<Value>>();
                ActionResult{
                    data: res,
                    error: FlinchError::None,
                    time_taken: format!("{:?}",ttk.elapsed()),
                }
            }
            Flql::DeletePointer(pointer, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let x = col.delete(pointer.to_string()).await;
                ActionResult{
                    data: vec![Value::String(pointer)],
                    error: FlinchError::None,
                    time_taken: x,
                }
            }
            Flql::DeleteClip(clip, collection) => {
                let ttk = Instant::now();
                let col = self.db.using(trim_apos(&collection).as_str());
                if col.is_err() {
                    return ActionResult{
                        data: vec![],
                        error: self.err_c(col.err()),
                        time_taken: format!("{:?}",ttk.elapsed()),
                    };
                }
                let col = col.unwrap();
                let x = col.delete_by_clip(clip.as_str()).await;
                ActionResult{
                    data: vec![Value::String(clip)],
                    error: FlinchError::None,
                    time_taken: x,
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

    async fn new_c(&self, options: String) ->Result<(), QueryError> {
        let parsed:serde_json::Result<CollectionOptions> = serde_json::from_str(options.as_str());
        if parsed.is_ok() {
            let opts = parsed.unwrap();
            let x = self.db.add(opts).await;
            if x.is_err() {
                return Err(QueryError::CollectionError(x.err().unwrap()));
            }
        } else {
            return Err(QueryError::ConfigureParseError(parsed.err().unwrap().to_string()));
        }
        Ok(())
    }

    fn proc(&self, data: &mut Vec<Value>, sort: Option<Sort>) {
        if let Some(option) = sort {
            data.par_sort_unstable_by(|kv1,kv2|{
                let k1 = kv1.get(option.field.as_str());
                if k1.is_some() {
                    let k1 = k1.unwrap().clone();
                    let k2 = kv2.get(option.field.as_str()).unwrap().clone();
                    let k1 = k1.as_str().unwrap();
                    let k2 = k2.as_str().unwrap();
                    return match option.direction {
                        SortDirection::Asc => k1.cmp(k2),
                        SortDirection::Desc => k2.cmp(k1),
                    };
                }
                Ordering::Equal
            });
        }
    }

    fn err_c(&self, error: Option<CollectionError>) -> FlinchError {
        if error.is_some() {
            let err = error.unwrap();
            debug!("collection error {} for query {}", &err, &self.current);
            FlinchError::CollectionError(err)
        } else {
            FlinchError::None
        }
    }

    fn err_q(&self, error: Option<QueryError>) -> FlinchError {
        if error.is_some() {
            let err = error.unwrap();
            debug!("query error {} for query {}", &err, &self.current);
            FlinchError::QueryError(err)
        } else {
            FlinchError::None
        }
    }

    fn err_s(&self, error: String) -> FlinchError {
        debug!("expression error {} for query {}", &error, &self.current);
        FlinchError::ExpressionError(error)
    }

    fn err_d(&self, error: Option<DocumentError>) -> FlinchError {
        if error.is_some() {
            let err = error.unwrap();
            debug!("document error {} for query {}", &err, &self.current);
            FlinchError::DocumentError(err)
        } else {
            FlinchError::None
        }
    }

    fn err_i(&self, error: Option<IndexError>) -> FlinchError {
        if error.is_some() {
            let err = error.unwrap();
            debug!("index error {} for query {}", &err, &self.current);
            FlinchError::IndexError(err)
        } else {
            FlinchError::None
        }
    }
}