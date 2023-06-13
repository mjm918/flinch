#[cfg(test)]
mod tests {
    use serde::{Serialize, Deserialize};
    use std::path::Path;
    use flinch::database::CollectionOptions;
    use flinch::doc_trait::ViewConfig;
    use flinch::headers::{DbUser, FlinchError};
    use flinch::schemas::Schemas;

    const COLLECTION: &str = "demo";

    #[derive(Serialize, Deserialize)]
    struct User {
        name: String,
        age: i64
    }

    #[tokio::test]
    async fn test() {
        let _ = std::fs::remove_dir_all(Path::new(".").join("data").as_path());

        let schema = Schemas::init().await;
        assert!(schema.is_ok());

        let schema = schema.unwrap();

        let login = schema.login("root","flinch","*");
        assert!(login.is_ok(), "{}", login.err().unwrap());

        let session_id = login.unwrap();
        assert!(!session_id.is_empty());

        let dummy_db = vec!["linux","macos","windows","android","/iphone-os"];
        for db in &dummy_db {
            let new_db = serde_json::to_string(&DbUser {
                name: format!("julfikar"),
                pw: "julfikar123@".to_string(),
                db: db.to_string(),
                create: true,
                drop: true,
                read: true,
                write: true,
                permit: true,
                flush: true,
            }).unwrap();

            let db_created = schema.flql(format!("db.new({});", new_db).as_str(), session_id.clone()).await;
            if db.eq(&"/iphone-os") {
                assert!(db_created.error.ne(&FlinchError::None),"{:?}",db_created);
            } else {
                assert!(db_created.error.eq(&FlinchError::None),"{:?}",db_created);
            }
        }

        let new_user = serde_json::to_string(&DbUser {
            name: format!("moe"),
            pw: "julfikar123@".to_string(),
            db: "windows".to_string(),
            create: true,
            drop: true,
            read: true,
            write: true,
            permit: true,
            flush: true,
        }).unwrap();
        let permit_another_user = schema.flql(format!("db.permit({});",new_user).as_str(), session_id.clone()).await;
        assert!(permit_another_user.error.eq(&FlinchError::None),"{:?}",permit_another_user);

        let drop_user = schema.flql("db('windows').user('meenie').drop();", session_id.clone()).await;
        assert!(drop_user.error.ne(&FlinchError::None),"{:?}",drop_user);

        let drop_user = schema.flql("db('windows').user('moe').drop();", session_id.clone()).await;
        assert!(drop_user.error.eq(&FlinchError::None),"{:?}",drop_user);


        let login = schema.login("julfikar","julfikar123@","macos");
        assert!(login.is_ok(), "{}", login.err().unwrap());

        let session_id = login.unwrap();
        assert!(!session_id.is_empty());


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
        let col_created = schema.flql(format!("new({});",options.as_str()).as_str(),session_id.clone()).await;
        assert!(col_created.error.eq(&FlinchError::None),"{:?}",col_created);

        let record_size = 7402;
        for k in 0..record_size {
            let v = serde_json::to_string(
                &User {
                    name: format!("julfikar{}",&k),
                    age: k,
                }
            ).unwrap();
            let query = format!("put({}).into('{}');", v, &COLLECTION);
            let x = schema.flql(query.as_str(), session_id.clone()).await;
            assert_eq!(x.error, FlinchError::None);
        }

        let login = schema.login("julfikar","julfikar123@","windows");
        assert!(login.is_ok(), "{}", login.err().unwrap());

        let session_id = login.unwrap();
        assert!(!session_id.is_empty());

        let res = schema.flql(format!("get.when('.name == \"julfikar100\"').from('{}').sort(null).page(null);",&COLLECTION).as_str(), session_id.clone()).await;
        assert_eq!(res.data.len(),0);


        let login = schema.login("julfikar","julfikar123@","macos");
        assert!(login.is_ok(), "{}", login.err().unwrap());

        let session_id = login.unwrap();
        assert!(!session_id.is_empty());

        let res = schema.flql(format!("get.when('.name == \"julfikar100\"').from('{}').sort(null).page(null);",&COLLECTION).as_str(), session_id.clone()).await;
        assert_ne!(res.data.len(),0);

        let login = schema.login("root","flinch","*");
        assert!(login.is_ok(), "{}", login.err().unwrap());

        let session_id = login.unwrap();
        assert!(!session_id.is_empty());

        for db in &dummy_db {
            let db_dropped = schema.flql(format!("db.drop('{}');",db).as_str(),session_id.clone()).await;
            if db.eq(&"/iphone-os") {
                assert!(db_dropped.error.ne(&FlinchError::None),"{:?}",db_dropped);
            } else {
                assert!(db_dropped.error.eq(&FlinchError::None),"{:?}",db_dropped);
            }
        }
    }
}