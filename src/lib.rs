mod hidx;
mod ividx;
mod range;
mod clips;
mod err;
mod hdrs;
mod wtch;
mod sess;
mod qry;
pub mod doc;
pub mod col;
pub mod db;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Instant};
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc::channel;
    use crate::doc::{Document, FromRawString, ViewConfig};
    use crate::db::{Database, CollectionOptions};
    use crate::hdrs::{Event, Query};

    #[tokio::test]
    async fn it_works() {
        #[derive(Serialize, Deserialize, Debug, Clone)]
        struct User {
            name: String,
            age: i64
        }
        let mut db = Database::<String, FromRawString>::init();

        assert!(db.create( CollectionOptions{
            name: "demo".to_string(),
            index_opts: vec![ "name".to_string() ],
            search_opts: vec![ "name".to_string() ],
            view_opts: vec![ ViewConfig{
                prop: "age".to_string(),
                expected: "123123".to_string(),
                view_name: "ME".to_string(),
            } ],
            range_opts: vec![ "age".to_string() ],
            clips_opts: vec![],
        }).is_ok());

        let store = db.using("demo").unwrap();
        let write_time = Instant::now();
        let iter = 100_000;
        for i in 0..iter {
            let user = User{ name: format!("Julfikar{}",i), age: i };
            store.collection.put(format!("P_0{}", i), FromRawString::new(
                serde_json::to_string(&user).unwrap().as_str()
            ).unwrap()).await.unwrap();
        }
        println!("{} records took {:?} to write",&iter,write_time.elapsed());

        let val = store.collection.search("Julfikar1".to_string());
        println!("Lookup 1 key in {} records . Found {:?} result(s) Execution Time {}",&iter,val.1.len(),val.0);

        println!("Total number of records {}", store.collection.len());

        let rng_search = store.collection.range("age", "100".to_string(), "105".to_string());
        println!("Range search count {} in {}", rng_search.1.len(), rng_search.0);

        store.collection.delete_by_range("age","100".to_string(),"100".to_string()).await;
        println!("Total number of records after deleting range {}", store.collection.len());

        let last_inserted_id = store.collection.id();
        store.collection.put(last_inserted_id.clone(),FromRawString::new(
            serde_json::to_string(&User { name: format!("Julfikar{}",123123), age: 123123 }).unwrap().as_str()
        ).unwrap()).await.unwrap();
        println!("Last inserted id {} single execution", last_inserted_id);

        let vw = store.collection.fetch_view("ME");
        println!("Fetch view {:?}", serde_json::to_string(&vw.1));
    }
}
