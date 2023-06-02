use std::cmp::Ordering;
use std::time::Instant;

use crossbeam_queue::SegQueue;
use flql::Flql;
use futures::executor::block_on;
use log::{debug, trace};
use rayon::prelude::*;
use serde_json::{Number, Value};
use tokio::sync::mpsc::Sender;

use crate::database::{CollectionOptions, Database};
use crate::doc::QueryBased;
use crate::doc_trait::Document;
use crate::errors::{CollectionError, DocumentError, IndexError, QueryError};
use crate::headers::{FlinchError, PubSubEvent, QueryResult, Sort, SortDirection};
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

    /// creates an instance of `Flinch` database with name
    pub async fn new_with_name(name: &str) -> Self {
        let db = Database::<QueryBased>::init_with_name(name).await;
        Self { db, current: "".to_string() }
    }

    pub fn underlying_db(&self) -> &Database<QueryBased> {
        &self.db
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
    pub async fn exec(&mut self, stmt: &str) -> QueryResult {
        trace!("flql executing {}", &stmt.chars().take(60).collect::<String>());

        self.current = format!("{}",&stmt);

        let parsed = flql::parse(stmt);
        if parsed.is_err() {
            return QueryResult {
                data: vec![],
                error: FlinchError::CustomError(parsed.err().unwrap()),
                time_taken: "".to_string(),
            };
        }

        let parsed = parsed.unwrap();
        self.exec_with_flql(parsed).await
    }

    /// expect a parsed `FLQL`
    pub async fn exec_with_flql(&self, parsed: Flql) -> QueryResult {
        match parsed {
            Flql::New(options) => self.col_new(options).await,
            Flql::Drop(collection) => self.col_drop(collection).await,
            Flql::Flush(collection) => self.col_flush(collection).await,
            Flql::Exists(pointer,collection) => self.pointer_exi(pointer, collection),
            Flql::Length(collection) => self.col_length(collection),
            Flql::Ttl(duration,condition,collection) => self.pointer_ttl(duration, condition, collection).await,
            Flql::Put(data, collection) => self.put_data(data, collection).await,
            Flql::PutWhen(data, condition, collection) => self.put_data_when(data, condition, collection).await,
            Flql::PutPointer(data, pointer, collection) => self.put_pointer(data, pointer, collection).await,
            Flql::SearchTyping(query, collection) => self.search_typing(query, collection),
            Flql::Get(collection,sort,limit) => self.fetch_all(collection, sort, limit),
            Flql::GetWhen(condition, collection,sort,limit) => self.fetch_when(condition, collection, sort, limit),
            Flql::GetPointer(pointer, collection) => self.get_pointer(pointer, collection),
            Flql::GetView(view, collection) => self.get_view(view, collection),
            Flql::GetClip(clip, collection) => self.get_clip(clip, collection),
            Flql::GetIndex(index, collection) => self.get_index(index, collection),
            Flql::GetRange(start, end, on, collection) => self.get_range(start, end, on, collection),
            Flql::Delete(collection) => self.delete_collection(collection).await,
            Flql::DeleteWhen(condition, collection) => self.delete_when(condition, collection).await,
            Flql::DeletePointer(pointer, collection) => self.delete_pointer(pointer, collection).await,
            Flql::DeleteClip(clip, collection) => self.delete_clip(clip, collection).await,
            _ => {
                QueryResult {
                    data: vec![],
                    error: FlinchError::None,
                    time_taken: "".to_string(),
                }
            }
        }
    }
    
    pub async fn col_new(&self, options: String) -> QueryResult {
        let ttk = Instant::now();
        let parsed:serde_json::Result<CollectionOptions> = serde_json::from_str(options.as_str());
        if parsed.is_ok() {
            let opts = parsed.unwrap();
            let x = self.db.add(opts).await;
            if x.is_err() {
                return QueryResult {
                    data: vec![],
                    error: self.err_q(Some(QueryError::CollectionError(x.err().unwrap()))),
                    time_taken: format!("{:?}",ttk.elapsed()),
                };
            }
        } else {
            return QueryResult {
                data: vec![],
                error: self.err_q(Some(QueryError::ConfigureParseError(parsed.err().unwrap().to_string()))),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        QueryResult {
            data: vec![],
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub async fn col_drop(&self, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let x = self.db.drop(collection.as_str()).await;
        QueryResult {
            data: vec![],
            error: self.err_c(x.err()),
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub async fn col_flush(&self, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_c(col.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = col.unwrap();
        col.flush_bkp().await;
        QueryResult {
            data: vec![],
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub fn col_length(&self, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_c(col.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = col.unwrap();
        let ttk = Instant::now();
        QueryResult {
            data: vec![Value::Number(Number::from(col.len()))],
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub fn pointer_exi(&self, pointer: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_c(col.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = col.unwrap();
        let ttk = Instant::now();
        let exi = col.get(&pointer);
        let res = if exi.data.is_some() {
            vec![Value::Bool(true)]
        } else {
            vec![Value::Bool(false)]
        };
        QueryResult {
            data: res,
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub async fn pointer_ttl(&self, duration: String, condition: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let timestamp = duration.parse::<i64>();
        if timestamp.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_s(format!("{} is malformed as a TTL value",duration)),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let expression = flql::expr_parse(trim_apos(&condition).as_str());
        if expression.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_s(expression.err().unwrap().to_string()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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
        QueryResult {
            data,
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub async fn put_data(&self, data: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let qdata = QueryBased::from_str(data.as_str());
        if qdata.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_d(qdata.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_c(col.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = col.unwrap();
        let ttk = Instant::now();
        let id = col.id();
        let x = col.put(id.clone(), qdata.unwrap()).await;
        QueryResult {
            data: vec![Value::String(id)],
            error: self.err_i(x.err()),
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub async fn put_data_when(&self, data: String, condition: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let expression = flql::expr_parse(trim_apos(&condition).as_str());
        if expression.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_s(expression.err().unwrap().to_string()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let qdata = QueryBased::from_str(data.as_str());
        if qdata.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_d(qdata.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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
        QueryResult {
            data: ids,
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub async fn put_pointer(&self, data: String, pointer: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let qdata = QueryBased::from_str(data.as_str());
        if qdata.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_d(qdata.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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
        QueryResult {
            data: vec![],
            error: FlinchError::None,
            time_taken,
        }
    }

    pub fn search_typing(&self, query: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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
        QueryResult {
            data,
            error: FlinchError::None,
            time_taken,
        }
    }

    pub fn fetch_all(&self, collection: String, sort: Option<String>, limit: Option<String>) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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

        QueryResult {
            data: if let Some((offset, limit)) = limit {
                data[offset..(offset + limit)].to_owned()
            } else {
                data
            },
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub fn fetch_when(&self, condition: String, collection: String, sort: Option<String>, limit: Option<String>) -> QueryResult {
        let ttk = Instant::now();
        let expression = flql::expr_parse(trim_apos(&condition).as_str());
        if expression.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_s(expression.err().unwrap().to_string()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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
        QueryResult {
            data: if let Some((offset, limit)) = limit {
                data[offset..(offset + limit)].to_owned()
            } else {
                data
            },
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub fn get_pointer(&self, pointer: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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
        QueryResult {
            data: res,
            error: FlinchError::None,
            time_taken,
        }
    }

    pub fn get_view(&self, view: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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
        QueryResult {
            data: res,
            error: FlinchError::None,
            time_taken,
        }
    }

    pub fn get_clip(&self, clip: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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
        QueryResult {
            data: res,
            error: FlinchError::None,
            time_taken,
        }
    }

    pub fn get_index(&self, index: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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
        QueryResult {
            data,
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub fn get_range(&self, start: String, end: String, on: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_c(col.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = col.unwrap();
        let ttk = Instant::now();
        let res = col.fetch_range(trim_apos(&on).as_str(), trim_apos(&start), trim_apos(&end));
        let data = res.data.iter().map(|kv|kv.1.make(kv.0.to_owned())).collect::<Vec<Value>>();
        QueryResult {
            data,
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub async fn delete_collection(&self, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_c(col.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = col.unwrap();
        let ttk = Instant::now();
        let _ = col.empty().await;
        QueryResult {
            data: vec![],
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed())
        }
    }

    pub async fn delete_when(&self, condition: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let expression = flql::expr_parse(trim_apos(&condition).as_str());
        if expression.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_s(expression.err().unwrap().to_string()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
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
        QueryResult {
            data: res,
            error: FlinchError::None,
            time_taken: format!("{:?}",ttk.elapsed()),
        }
    }

    pub async fn delete_pointer(&self, pointer: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_c(col.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = col.unwrap();
        let x = col.delete(pointer.to_string()).await;
        QueryResult {
            data: vec![Value::String(pointer)],
            error: FlinchError::None,
            time_taken: x,
        }
    }

    pub async fn delete_clip(&self, clip: String, collection: String) -> QueryResult {
        let ttk = Instant::now();
        let col = self.db.using(trim_apos(&collection).as_str());
        if col.is_err() {
            return QueryResult {
                data: vec![],
                error: self.err_c(col.err()),
                time_taken: format!("{:?}",ttk.elapsed()),
            };
        }
        let col = col.unwrap();
        let x = col.delete_by_clip(clip.as_str()).await;
        QueryResult {
            data: vec![Value::String(clip)],
            error: FlinchError::None,
            time_taken: x,
        }
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