#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use flinch::db::CollectionOptions;
    use flinch::doc::ViewConfig;
    use flinch::hdrs::FlinchError;
    use flinch::qry::Query;

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
        let options = serde_json::to_string(&col_opts).unwrap();
        let planner = Query::new();
        let res = planner.exec(format!("new({});",options.as_str()).as_str()).await;
        println!("new::collection::error {:?}",res.error);

        let record_size = 1000;//1_000_000;
        for i in 0..record_size {
            let v = serde_json::to_string(
                &User {
                    name: format!("julfikar{}",&i),
                    age: i,
                }
            ).unwrap();
            let query = format!("put({}).into('{}');", v, &COLLECTION);
            let x = planner.exec(query.as_str()).await;
            assert_eq!(x.error, FlinchError::None);
        }

        let res = planner.exec(format!("get.when('.name == \"julfikar100\"').from('{}');",&COLLECTION).as_str()).await;
        println!("when::map:: {:?} {}",res.time_taken,res.data.len());
        assert_ne!(res.data.len(),0);

        let res = planner.exec(format!("get.index('julfikar1').from('{}');",&COLLECTION).as_str()).await;
        println!("get::index::{:?} {}",res.time_taken,res.data.len());
        assert_eq!(res.data.len(),1);

        let res = planner.exec(format!("get.when('.name CONTAINS \"julfikar\" && .name CONTAINS \"1\"').from('{}').sort('name','ASC').page(0,10);",&COLLECTION).as_str()).await;
        println!("get::when::{:?} {:?}",res.time_taken,res.data.len());
        assert_ne!(res.data.len(),0);

        let res = planner.exec(format!("get.when('.age == 100 && COERCE .name _lowercase_ CONTAINS \"julfikar\"').from('{}');",&COLLECTION).as_str()).await;
        println!("get::when::{:?} {}",res.time_taken,res.data.len());
        assert_ne!(res.data.len(),0);
    }
}