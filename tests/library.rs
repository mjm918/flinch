#[cfg(test)]
mod tests {
    use std::time::Instant;
    use log::debug;
    use serde::{Deserialize, Serialize};
    use flinch::database::{CollectionOptions, Database};
    use flinch::doc_trait::{Document, ViewConfig};
    use flinch::doc::QueryBased;
    use flinch::headers::{NotificationType, PubSubEvent};

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
        let database: Database<QueryBased> = Database::init().await;
        let _ = database.add(col_opts).await;

        debug!("ls Collections {:?}", database.ls());

        let instance = database.using(COLLECTION);
        assert!(instance.is_ok());

        let instance = instance.unwrap();
        let collection = instance.value();

        let (sx, mut rx) = tokio::sync::mpsc::channel(30000);
        collection.sub(sx).await.expect("subscribe to channel");

        let insert = Instant::now();
        let record_size = 7000;
        for k in 0..record_size {
            let v = serde_json::to_string(
                &User {
                    name: format!("julfikar{}",&k),
                    age: k,
                }
            ).unwrap();
            collection.put(collection.id(), QueryBased::from_str(v.as_str()).unwrap()).await.unwrap();
        }
        assert_ne!(collection.len(),0);
        debug!("insert:: {:?}",insert.elapsed());
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
        debug!("replaced value in {}",x.unwrap());

        let single = collection.get(&format!("P_0"));
        assert!(single.data.is_some());
        debug!("single:: {:?}", single.time_taken);

        let multi = collection.multi_get(vec![&format!("P_1"),&format!("P_0")]);
        assert_ne!(multi.data.len(),0);
        debug!("multi:: {:?}",multi.time_taken);

        let gidx = collection.get_index("julfikar100");
        assert!(gidx.data.is_some());
        debug!("index:: {:?} {:?}",gidx.time_taken,gidx.data.unwrap().1.data);

        let search = collection.search("Julfikar1");
        assert_ne!(search.data.len(),0);
        debug!("search index:: {} res {}",search.time_taken, search.data.len());

        let like_search = collection.like_search("Julfikar 101");
        assert_ne!(like_search.data.len(),0);
        debug!("search:: {} res {}",like_search.time_taken, like_search.data.len());

        let view = collection.fetch_view("ADULT");
        assert_ne!(view.data.len(),0);
        debug!("view:: {} res {}",view.time_taken, view.data.len());

        collection.empty().await;
        assert_eq!(collection.len(),0,"after::drop");

        let mut i = 0;
        loop {
            let event = rx.recv().await.unwrap();
            match event {
                PubSubEvent::Data(d) => {
                    match d {
                        NotificationType::Insert(k, _v) => {
                            println!("inserted :pub/sub: {}",k);
                        }
                        NotificationType::Remove(k) => {
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