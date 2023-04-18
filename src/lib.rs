mod hidx;
mod ividx;
mod range;
mod clips;
mod err;
mod hdrs;
mod wtch;
mod sess;
mod act;
pub mod doc;
pub mod col;
pub mod db;
mod qry;
mod utils;

#[cfg(test)]
mod tests {
    use std::fmt::format;
    use std::sync::Arc;
    use std::time::{Instant};
    use lazy_static::lazy_static;
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc::channel;
    use crate::doc::{Document, FromRawString, ViewConfig};
    use crate::db::{Database, CollectionOptions};
    use crate::hdrs::{Event, ActionType};

    const COLLECTION: &str = "demo";
    #[derive(Serialize, Deserialize)]
    struct User {
        name: String,
        age: i64
    }
    #[tokio::test]
    async fn it_works() {
        let mut db : Database<String, FromRawString> = Database::init();
        assert!(db.create(CollectionOptions{
            name: COLLECTION.to_string(),
            index_opts: vec![format!("name")],
            search_opts: vec![format!("name")],
            view_opts: vec![ViewConfig{
                prop: "age".to_string(),
                expected: "18".to_string(),
                view_name: "ADULT".to_string(),
            }],
            range_opts: vec![format!("age")],
            clips_opts: vec![format!("name")],
        }).is_ok());

        let instance = db.using(COLLECTION);
        assert!(instance.is_ok());

        let instance = instance.unwrap();
        let collection = instance.value();
        assert_eq!(collection.len(),0);

        let insert = Instant::now();
        let record_size = 100_000;
        for i in 0..record_size {
            collection.put(format!("P_{}",&i), FromRawString::new(
                serde_json::to_string(
                    &User {
                        name: format!("julfikar{}",&i),
                        age: i,
                    }
                ).unwrap().as_str()
            ).unwrap()).await.unwrap();
        }
        assert_eq!(collection.len(),record_size as usize);
        println!("insert:: {:?}",insert.elapsed());

        let single = collection.get(&format!("P_0"));
        assert!(single.data.is_some());
        println!("single:: {:?}", single.time_taken);

        let multi = collection.multi_get(vec![&format!("P_100"),&format!("P_1999")]);
        assert_eq!(multi.data.len(),2);
        println!("multi:: {:?}",multi.time_taken);

        let search = collection.search("Julfikar9999");
        assert_ne!(search.data.len(),0);
        println!("search:: {} res {}",search.time_taken, search.data.len());

        let like_search = collection.like_search("Julfikar 99 11");
        assert_ne!(like_search.data.len(),0);
        println!("search:: {} res {}",like_search.time_taken, like_search.data.len());

        let view = collection.fetch_view("ADULT");
        assert_ne!(view.data.len(),0);
        println!("view:: {} res {}",view.time_taken, view.data.len());
    }
}
