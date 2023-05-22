#[cfg(test)]
mod tests {
    use std::time::Instant;
    use serde::{Deserialize, Serialize};
    use flinch::db::{CollectionOptions, Database};
    use flinch::doc::{Document, ViewConfig};
    use flinch::docv::QueryBased;
    use flinch::hdrs::{ActionType, PubSubEvent};

    const COLLECTION: &str = "demo";
    #[derive(Serialize, Deserialize)]
    struct User {
        name: String,
        age: i64
    }
    #[tokio::test]
    async fn library() {
        let col_opts = CollectionOptions {
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
        };
        let database = Database::init();
        database.add(col_opts).expect("created new collection");

        println!("ls Collections {:?}", database.ls());

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
        // collection.load_bkp().await;

        let x = collection.put(format!("P_{}",0), QueryBased::from_str(
            serde_json::to_string(
                &User {
                    name: format!("julfikar0"),
                    age: 100100,
                }
            ).unwrap().as_str()
        ).unwrap()).await;
        assert!(x.is_ok());
        println!("replaced value in {}",x.unwrap());

        let single = collection.get(&format!("P_0"));
        assert!(single.data.is_some());
        println!("single:: {:?}", single.time_taken);

        let multi = collection.multi_get(vec![&format!("P_1"),&format!("P_0")]);
        assert_ne!(multi.data.len(),0);
        println!("multi:: {:?}",multi.time_taken);

        let gidx = collection.get_index("julfikar100");
        assert!(gidx.data.is_some());
        println!("index:: {:?} {:?}",gidx.time_taken,gidx.data.unwrap().1.data);

        let search = collection.search("Julfikar1");
        assert_ne!(search.data.len(),0);
        println!("search index:: {} res {}",search.time_taken, search.data.len());

        let like_search = collection.like_search("Julfikar 101");
        assert_ne!(like_search.data.len(),0);
        println!("search:: {} res {}",like_search.time_taken, like_search.data.len());

        let view = collection.fetch_view("ADULT");
        assert_ne!(view.data.len(),0);
        println!("view:: {} res {}",view.time_taken, view.data.len());

        collection.empty().await;
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
}