#[cfg(test)]
mod tests {
    use log::debug;
    use serde::{Deserialize, Serialize};
    use flinch::database::CollectionOptions;
    use flinch::doc_trait::ViewConfig;
    use flinch::headers::{NotificationType, FlinchError, PubSubEvent};
    use flinch::query::Query;

    const COLLECTION: &str = "demo";
    #[derive(Serialize, Deserialize)]
    struct User {
        name: String,
        age: i64
    }

    #[tokio::test]
    async fn query() {
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
        // let (sx, mut _rx) = tokio::sync::mpsc::channel(30000);
        let options = serde_json::to_string(&col_opts).unwrap();
        let mut planner = Query::new().await;
        let res = planner.exec(format!("new({});",options.as_str()).as_str()).await;
        debug!("new::collection::error {:?}",res.error);

        // planner.subscribe(COLLECTION,sx).await.expect("subscribe channel");

        let record_size = 7402;
        for k in 0..record_size {
            let v = serde_json::to_string(
                &User {
                    name: format!("julfikar{}",&k),
                    age: k,
                }
            ).unwrap();
            let query = format!("put({}).into('{}');", v, &COLLECTION);
            let x = planner.exec(query.as_str()).await;
            assert_eq!(x.error, FlinchError::None);
        }

        let x = planner.exec(format!("ttl(1).if('.age > 100 && .age < 110').into('{}');",&COLLECTION).as_str()).await;
        debug!("ttl::when:: {:?}",x.data);
        assert_eq!(x.error,FlinchError::None);

        let res = planner.exec(format!("get.when('.name == \"julfikar100\"').from('{}').sort(null).page(null);",&COLLECTION).as_str()).await;
        debug!("when::map:: {:?} {:?}",res.time_taken,res.data);
        assert_ne!(res.data.len(),0);

        let res = planner.exec(format!("get.index('julfikar1').from('{}');",&COLLECTION).as_str()).await;
        debug!("get::index::{:?} {}",res.time_taken,res.data.len());
        assert_eq!(res.data.len(),1);

        let res = planner.exec(format!("get.when('.name CONTAINS \"julfikar\" && .name CONTAINS \"1\"').from('{}').sort('name','ASC').page(100,11);",&COLLECTION).as_str()).await;
        debug!("get::when:(sort,limit):{:?} {:?}",res.time_taken,res.data.len());
        assert_ne!(res.data.len(),0);

        let res = planner.exec(format!("get.when('.name CONTAINS \"julfikar\" && .name CONTAINS \"101\"').from('{}').sort(null).page(null);",&COLLECTION).as_str()).await;
        debug!("get::when:(no-sort,no-limit):{:?} {:?}",res.time_taken,res.data.len());
        assert_ne!(res.data.len(),0);

        let res = planner.exec(format!("get.from('{}').sort(null).page(10,1);",&COLLECTION).as_str()).await;
        debug!("get::all::{:?} {:?}",res.time_taken,res.data.len());
        assert_ne!(res.data.len(),0);

        let res = planner.exec(format!("get.range(start:'10',end:'100',on:'age').from('{}');",&COLLECTION).as_str()).await;
        debug!("get::range::{:?} {:?}",res.time_taken,res.data);
        assert_ne!(res.data.len(),0);

        /*let mut i = 0;
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
        }*/
    }
}