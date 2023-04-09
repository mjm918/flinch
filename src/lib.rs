mod hidx;
mod doc;
mod ividx;
mod range;
mod clips;
mod err;
mod hdrs;
mod wtch;
mod sess;
mod store;
mod qry;
mod flinch;


#[cfg(test)]
mod tests {
    use std::time::Instant;
    use serde::{Deserialize, Serialize};
    use crate::doc::{Document, FromRawString};
    use crate::flinch::{Flinch, StoreOptions};

    #[tokio::test]
    async fn it_works() {
        #[derive(Serialize, Deserialize, Debug, Clone)]
        struct User {
            name: String,
            age: i64
        }
        let flinch = Flinch::<String, FromRawString>::init();
        assert!(flinch.create(StoreOptions{
            name: "first".to_string(),
            index_opts: vec![],
            search_opts: vec!["name".to_string()],
            view_opts: None,
            range_opts: vec![],
            clips_opts: vec![],
        }).is_ok());
        let db = flinch.using("first");
        let write_time = Instant::now();
        for i in 0..100_000 {
            let user = User{ name: format!("Julfikar_{}",i), age: i };
            db.put(format!("P_0{}",i), FromRawString::new(
                serde_json::to_string(&user).unwrap().as_str()
            ).unwrap()).await.unwrap();
        }
        println!("10_000 records took {:?} to write",write_time.elapsed());
        let val = db.wildcard_search("Julfikar 111".to_string()); // slow but users will like it
        // type as you go is db.search <--- Its super fast
        println!("Lookup 1 key in 10_000 records {:?} Execution Time {}",val.1.len(),val.0);
    }
}
