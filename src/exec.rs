use anyhow::{Error, Result, anyhow};
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use dql::{Clause, Dql, OffsetLimit, SortDirection};
use serde_json::{Number, Value};
use crate::col::Collection;
use crate::db::CollectionOptions;
use crate::doc::Document;
use crate::docv::QueryBased;
use crate::hdrs::QueryResult;

pub fn execute(db: &DashMap<String, Collection<String, QueryBased>>, ql: &str) -> QueryResult {
    let mut res = QueryResult {
        query: "".to_string(),
        data: vec![],
        errors: vec![],
        time_taken: "".to_string(),
    };

    let parsed = dql::parse(ql);
    if parsed.is_ok() {
        let parsed = parsed.unwrap();
        for dql in parsed {
            match dql {
                Dql::Create(name, option) =>{},
                Dql::Drop(name) =>{},
                Dql::Len(name) =>{},
                Dql::Upsert(name, doc, clause) =>{},
                Dql::UpsertWithoutClause(name, doc) =>{},
                Dql::Put(name, index, doc) =>{},
                Dql::Exists(name, index) =>{},
                Dql::Search(name, query, sort, limit) =>{},
                Dql::GetIndex(name, index) =>{},
                Dql::GetWithoutClause(name, sort, limit) =>{},
                Dql::Get(name, clause, sort, limit) =>{},
                Dql::DeleteIndex(name, index) =>{},
                Dql::DeleteWithoutClause(name) =>{},
                Dql::Delete(name, clause) =>{},
                Dql::None => {
                    res.errors.push(format!("failed to parse query"));
                }
            }
        }
    } else {
        res.errors.push(format!("{}",parsed.err().unwrap()));
    }

    res
}

fn create_c(db: &DashMap<String, Collection<String, QueryBased>>, name: &str, option: Value) -> Result<()> {
    if col_exists(&db, &name) {
        return Err(anyhow!("collection exists"));
    }
    let opt: serde_json::error::Result<CollectionOptions> = serde_json::from_value(option);
    if opt.is_err() {
        return Err(anyhow!("collection configuration is not valid"));
    }
    db.insert(name.to_string(), Collection::new(opt.unwrap()));
    Ok(())
}

fn drop_c(db: &DashMap<String, Collection<String, QueryBased>>, name: &str) -> Result<()> {
    let col = col(&db,&name);
    if col.is_err() {
        return Err(anyhow!("collection does not exist"));
    }
    let col = col.unwrap().value();
        col.drop();
    db.remove(name);

    Ok(())
}

fn length(db: &DashMap<String, Collection<String, QueryBased>>, name: &str) -> Result<Value> {
    let col = col(&db,&name);
    if col.is_err() {
        return Err(anyhow!("collection does not exist"));
    }
    Ok(Value::Number(Number::from(col.unwrap().value().len())))
}

async fn upsert(db: &DashMap<String, Collection<String, QueryBased>>, name: &str, doc:&Value) -> Result<()> {
    let col = col(&db,&name);
    if col.is_err() {
        return Err(anyhow!("collection does not exist"));
    }
    let col = col.unwrap().value();
    let d = QueryBased::from_value(doc);
    let mut d = d.unwrap();
        d.set_opts(&col.opts);
    let _ = col.put(col.id(),d).await;
    Ok(())
}

fn col_exists(db: &DashMap<String, Collection<String, QueryBased>>, name:&str) -> bool {
    db.get(name).is_some()
}

fn col(db: &DashMap<String, Collection<String, QueryBased>>, name:&str) -> Result<Ref<String, Collection<String, QueryBased>>> {
    if col_exists(db,name.clone()) {
        return Ok(db.get(name).unwrap());
    }
    Err(anyhow!("collection does not exists"))
}