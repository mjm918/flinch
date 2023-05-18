mod hidx;
mod ividx;
mod range;
mod clips;
mod err;
mod hdrs;
mod wtch;
mod pbsb;
mod act;
pub mod doc;
pub mod col;
pub mod db;
mod utils;
mod docv;
mod plnr;

///
/// Examples
/// 1M records
/// insert:: 61.118093292s
/// single:: 3.25µs
/// multi:: 6.458µs
/// search index:: 27.209µs result count 1
/// search:: 2.494042417s result count 402130
/// view:: 45.084µs result count 1
/// query:: 438.283791ms result count 10 with query {"age":{"$lt":10}}
///
#[cfg(test)]
mod tests {
    use std::time::Instant;
    use serde::{Deserialize, Serialize};
    use crate::doc::{Document, ViewConfig};
    use crate::db::{CollectionOptions, Database, DatabaseWithQuery};
    use crate::docv::QueryBased;
    use crate::hdrs::{ActionType, PubSubEvent};

    const COLLECTION: &str = "demo";
    #[derive(Serialize, Deserialize)]
    struct User {
        name: String,
        age: i64
    }
    #[tokio::test]
    async fn library() {
        let col_opts = CollectionOptions {
            name: Some(COLLECTION.to_string()),
            index_opts: vec![format!("name")],
            search_opts: vec![format!("name")],
            view_opts: vec![ViewConfig{
                prop: "age".to_string(),
                expected: "18".to_string(),
                view_name: "ADULT".to_string(),
            }],
            range_opts: vec![format!("age")],
            clips_opts: vec![format!("name")],
        };
        let database = Database::init();
        database.add(col_opts).expect("created new collection");

        let instance = database.using(COLLECTION);
        assert!(instance.is_ok());

        let instance = instance.unwrap();
        let collection = instance.value();
        assert_eq!(collection.len(),0);

        let (sx, mut rx) = tokio::sync::mpsc::channel(30000);
        collection.sub(sx).await.expect("subscribe to channel");

        let insert = Instant::now();
        let record_size = 10_000;
        for i in 0..record_size {
            collection.put(format!("P_{}",&i), QueryBased::from_str(
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

        let multi = collection.multi_get(vec![&format!("P_1"),&format!("P_0")]);
        assert_ne!(multi.data.len(),0);
        println!("multi:: {:?}",multi.time_taken);

        let search = collection.search("Julfikar0");
        assert_ne!(search.data.len(),0);
        println!("search index:: {} res {}",search.time_taken, search.data.len());

        let like_search = collection.like_search("Julfikar 101");
        assert_ne!(like_search.data.len(),0);
        println!("search:: {} res {}",like_search.time_taken, like_search.data.len());

        let view = collection.fetch_view("ADULT");
        assert_ne!(view.data.len(),0);
        println!("view:: {} res {}",view.time_taken, view.data.len());

        collection.drop().await;

        assert_eq!(collection.len(),0,"after::drop");

        let mut i = 0;
        loop {
            let event = rx.recv().await.unwrap();
            match event {
                PubSubEvent::Data(d) => {
                    match d {
                        ActionType::Insert(k, _v) => {
                            println!("inserted :pub/sub: {}",k);
                        }
                        ActionType::Remove(k) => {
                            println!("removed :: {}",k);
                        }
                    };
                }
                PubSubEvent::Subscribed(_s) => {

                }
            };
            i += 1;
            if i == 10 { // for demo, listen till 10 message only
                break;
            }
        }
    }
    // #[tokio::test]
    async fn query() {
        let col_opts = CollectionOptions {
            name: Some(COLLECTION.to_string()),
            index_opts: vec![format!("name")],
            search_opts: vec![format!("name")],
            view_opts: vec![ViewConfig{
                prop: "age".to_string(),
                expected: "18".to_string(),
                view_name: "ADULT".to_string(),
            }],
            range_opts: vec![format!("age")],
            clips_opts: vec![format!("name")],
        };
        let instance = DatabaseWithQuery::new();
        let database = instance.planner;
        let x = database.exec(format!("create collection -> {};",serde_json::to_string(&col_opts).unwrap()).as_str());
        println!("{:?}",x);

        /*let ttk = Instant::now();
        let planner = session.query(r#"{"$set":{"document":[{"a":"1"}],"filter":{"product":1}}}"#);
        println!("{} hmm...{:?}",planner.is_ok(), ttk.elapsed());*/
    }
}
