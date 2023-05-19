<img src="assets/flinch.png">

[![Rust](https://github.com/mjm918/flinch/actions/workflows/rust.yml/badge.svg)](https://github.com/mjm918/flinch/actions/workflows/rust.yml)

Flinch is an in-memory, real-time document database designed for fast, efficient full-text search and querying. It comes with a built-in full-text search engine that enables both "search-as-you-type" and like search capabilities. Flinch was created with the goal of providing a high-performance search solution that can be integrated into various applications.

# Features
- In-memory database: Flinch stores documents in memory, allowing for ultra-fast search performance.
- Real-time updates: Flinch supports real-time updates, enabling users to add, update, and delete documents in real-time.
- Full-text search: Flinch has a built-in full-text search engine that provides powerful search capabilities, including search-as-you-type and wildcard search.
- Lightweight and easy to use: Flinch is designed to be lightweight and easy to use, with a simple API that allows developers to quickly integrate it into their applications.
- Document-oriented: Flinch is document-oriented, allowing users to store and retrieve documents as JSON objects.
- Highly scalable: Flinch is designed to be highly scalable, allowing users to handle large volumes of documents and queries efficiently.
- Query: Document query faster than ⚡️
- Open source: Flinch is an open-source project, allowing users to contribute to its development and customize it to suit their needs.

# How to use

**As library**
```
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

    let x = collection.put(format!("P_{}",0), QueryBased::from_str(
        serde_json::to_string(
            &User {
                name: format!("julfikar-replace"),
                age: 10000,
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
```
**Query example**
```
async fn query_example() {
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
    let options = serde_json::to_string(&col_opts).unwrap();
    let planner = Query::new();
    let res = planner.exec(format!("new({});",options.as_str()).as_str()).await;
    println!("new::collection::error {:?}",res.error);

    let record_size = 2;
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

    let res = planner.exec(format!("get.when(:map(\"name\") == \"julfikar1\":).from('{}');",&COLLECTION).as_str()).await;
    println!("{:?}",res);

    let res = planner.exec(format!("get.index('julfikar1').from('{}');",&COLLECTION).as_str()).await;
    println!("{:?}",res);

    let res = planner.exec(format!("search.query('julfikar 1').from('{}');",&COLLECTION).as_str()).await;
    println!("{:?}",res);

    let res = planner.exec(format!("search.when(:map(\"age\") == 0:).query('julfikar').from('{}');",&COLLECTION).as_str()).await;
    println!("{:?}",res);
}
```

# Introducing FLQL 

> Create collection <br>
`new({});` <br>

> Drop collection <br>
`drop('');` <br>

> Check if pointer exists in collection <br>
`exists('').into('');` <br>

> Length of collection <br>
`length('');` <br>

> Update or Insert into collection <br>
`put({}).into('');` <br>

> Conditional Update or Insert into collection <br>
`put({}).when(:includes(array_filter('e.f$.g'),2):).into('');` <br>

> Update or Insert into collection to a Pointer <br>
`put({}).pointer('').into('');` <br>

> Get from collection <br>
`get.from('');` <br>

> Conditional Get from collection <br>
`get.when(:includes(array_filter('e.f$.g'),2):).from('');` <br>

> Get Pointer from collection <br>
`get.pointer('').from('');` <br>

> Get View from collection <br>
`get.view('').from('');` <br>

> Get Clip from collection <br>
`get.clip('').from('');` <br>

> Get index from collection <br>
`get.index('').from('');` <br>

> Get range from collection <br>
`get.range(start:'', end:'', on:'').from('');` <br>

> Search query <br>
`search.query('').from('');` <br>

> Conditional Search query <br>
`search.when(#func(args)#).query('').from('');` <br>

> Delete from collection <br>
`delete.from('');` <br>

> Conditional Delete from collection <br>
`delete.when(:includes(array_filter('e.f$.g'),2):).from('');` <br>

> Delete Pointer from collection <br>
`delete.pointer('').from('');` <br>

> Delete View from collection <br>
`delete.view('').from('');` <br>

>Delete Clip from collection <br>
`delete.clip('').from('');` <br>