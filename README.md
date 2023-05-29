<img src="assets/flinch.png">

[![Rust](https://github.com/mjm918/flinch/actions/workflows/rust.yml/badge.svg)](https://github.com/mjm918/flinch/actions/workflows/rust.yml)

# Flinch

Flinch is an in-memory, real-time document database designed for fast, efficient full-text search and querying. It provides a high-performance search solution that can be seamlessly integrated into various applications. Flinch is lightweight, easy to use, and open-source, allowing users to contribute to its development and customize it to suit their needs.

## Features

- **In-memory database**: Flinch stores documents in memory, enabling ultra-fast search performance.
- **Real-time updates**: Users can add, update, and delete documents in real-time, ensuring the database reflects the latest changes.
- **Full-text search**: Flinch includes a built-in full-text search engine that offers powerful search capabilities, including "search-as-you-type" and wildcard search.
- **Lightweight and easy to use**: Flinch is designed to be lightweight and has a simple API, making it easy for developers to integrate into their applications.
- **Document-oriented**: Flinch is a document-oriented database, allowing users to store and retrieve documents as JSON objects.
- **Highly scalable**: Flinch is built to handle large volumes of documents and queries efficiently, ensuring scalability as your application grows.
- **Query**: Flinch offers high-speed document querying capabilities, delivering query performance faster than lightning (⚡️).
- **Open source**: Flinch is an open-source project, enabling users to contribute to its development and customize it according to their requirements.

> Note: Insertion performance may be slow due to the time taken for indexing, which is a normal occurrence.

## How to Use

These examples demonstrate the capabilities of Flinch as both a library and a query language, showcasing its features for document storage, retrieval, manipulation, and searching.

## Example 1: Using Flinch as a Library

The first example demonstrates how to use Flinch as a library in your Rust application. It showcases various operations such as adding documents, updating values, retrieving data, performing searches, and more.

Here are the key operations performed in the code:

1. **Creating a Collection**: A collection is created with specific options, including index options, search options, view options, range options, and clip options.

2. **Adding Documents**: Documents are added to the collection using the `put` operation. Each document is represented as a JSON object, and it is assigned a unique key.

3. **Real-time Updates**: The code demonstrates real-time updates by subscribing to a channel and receiving events for document insertions and removals.

4. **Retrieving Documents**: The code shows how to retrieve documents using the `get` operation. It includes examples of getting a single document, getting multiple documents, and fetching documents based on an index.

5. **Searching**: The code demonstrates the search capabilities of Flinch. It shows how to perform a search query and retrieve documents that match the search query. It also showcases a "like" search using wildcard characters.

6. **Views**: Flinch supports views, which are predefined filters that can be applied to a collection. The code shows how to fetch documents based on a view configuration.

7. **Dropping a Collection**: The code demonstrates how to drop a collection, removing all its documents.

8. **Subscribing to Pub/Sub Events**: The code sets up a subscription to receive Pub/Sub events for document insertions and removals. It shows an example of listening to a limited number of events.

```rust
async fn library() {
    // Initialize Flinch with collection options
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

    // List available collections
    println!("ls Collections {:?}", database.ls());

    // Access the Flinch collection instance
    let instance = database.using(COLLECTION);
    assert!(instance.is_ok());
    let instance = instance.unwrap();
    let collection = instance.value();

    // Subscribe to real-time updates
    let (sx, mut rx) = tokio::sync::mpsc::channel(30000);
    collection.sub(sx).await.expect("subscribe to channel");

    // Insert documents into the collection
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
    assert_eq!(collection.len(), record_size as usize);
    println!("insert:: {:?}", insert.elapsed());

    // Replace a document in the collection
    let x = collection.put(format!("P_{}",0), QueryBased::from_str(
        serde_json::to_string(
            &User {
                name: format!("julfikar-replace"),
                age: 10000,
            }
        ).unwrap().as_str()
   

 ).unwrap()).await;
    assert!(x.is_ok());
    println!("replaced value in {}", x.unwrap());

    // Get a single document from the collection
    let single = collection.get(&format!("P_0"));
    assert!(single.data.is_some());
    println!("single:: {:?}", single.time_taken);

    // Get multiple documents from the collection
    let multi = collection.multi_get(vec![&format!("P_1"), &format!("P_0")]);
    assert_ne!(multi.data.len(), 0);
    println!("multi:: {:?}", multi.time_taken);

    // Get an index from the collection
    let gidx = collection.get_index("julfikar100");
    assert!(gidx.data.is_some());
    println!("index:: {:?} {:?}", gidx.time_taken, gidx.data.unwrap().1.data);

    // Perform a search query in the collection
    let search = collection.search("Julfikar0");
    assert_ne!(search.data.len(), 0);
    println!("search index:: {} res {}", search.time_taken, search.data.len());

    // Perform a like search query in the collection
    let like_search = collection.like_search("Julfikar 101");
    assert_ne!(like_search.data.len(), 0);
    println!("search:: {} res {}", like_search.time_taken, like_search.data.len());

    // Fetch a view from the collection
    let view = collection.fetch_view("ADULT");
    assert_ne!(view.data.len(), 0);
    println!("view:: {} res {}", view.time_taken, view.data.len());

    // Drop the collection
    collection.drop().await;
    assert_eq!(collection.len(), 0, "after::drop");

    // Listen to pub/sub events for demonstration (limited to 10 messages)
    let mut i = 0;
    loop {
        let event = rx.recv().await.unwrap();
        match event {
            PubSubEvent::Data(d) => {
                match d {
                    ActionType::Insert(k, _v) => {
                        println!("inserted :pub/sub: {}", k);
                    }
                    ActionType::Remove(k) => {
                        println!("removed :: {}", k);
                    }
                };
            }
            PubSubEvent::Subscribed(_s) => {

            }
        };
        i += 1;
        if i == 10 {
            break;
        }
    }
}
```

## Example 2: Querying with FLQL

The second example introduces FLQL (Flinch Query Language), which provides a powerful and concise syntax for interacting with Flinch collections. It demonstrates various FLQL queries and their functionalities.

Here's a breakdown of the FLQL queries showcased in the code:

1. **Creating a Collection**: This query creates a new collection with empty options.

2. **Dropping a Collection**: This query drops a collection from the database.

3. **Checking Pointer Existence**: The query checks if a specific pointer exists within a collection.

4. **Collection Length**: This query returns the length (number of documents) in a collection.

5. **Updating or Inserting Documents**: The query updates or inserts a document into a collection.

6. **Conditional Update or Insert**: This query performs a conditional update or insert based on a specified condition.

7. **Updating or Inserting to a Pointer**: The query updates or inserts a document into a pointer within a collection.

8. **Getting Documents**: This query retrieves documents from a collection.

9. **Conditional Get**: This query performs a conditional get operation based on a specified condition.

10. **Getting Pointer**: The query retrieves the value of a pointer from a collection.

11. **Getting View**: This query fetches documents based on a view configuration.

12. **Getting Clip**: This query fetches documents based on a clip configuration.

13. **Getting Index**: The query retrieves documents based on an index.

14. **Getting Range**: This query retrieves a range of documents based on specified start and end values.

15. **Searching**: This query performs a search query in the collection.

16. **Conditional Search**: This query performs a conditional search based on a specified condition.

17. **Deleting Documents**: This query deletes documents from a collection.

18. **Conditional Delete**: This query performs a conditional delete based on a specified condition.

19. **Deleting Pointer**: The query deletes a specific pointer from a collection.

20. **Deleting View**: This query deletes a view from a collection.

21. **Deleting Clip**: This query deletes a clip from a collection.

```rust
async fn query_example() {
    // Initialize Flinch with collection options
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

    // Create a new collection
    let res = planner.exec(format!("new({});",options.as_str()).as_str()).await;
    println!("new::collection::error {:?}",res.error);

    // Insert documents into the collection
    let record_size = 2;
    for i in 0..record_size {
        let v = serde_json::to_string(
            &User {
                name: format!("julfikar{}",&i),
                age: i,
            }
        ).unwrap();
        let query = format!("put({

}).into('{}');", v, &COLLECTION);
        let x = planner.exec(query.as_str()).await;
        assert_eq!(x.error, FlinchError::None);
    }

    // Get documents from the collection
    let res = planner.exec(format!("get.when(:map(\"name\") == \"julfikar1\":).from('{}');",&COLLECTION).as_str()).await;
    println!("{:?}",res);

    // Get an index from the collection
    let res = planner.exec(format!("get.index('julfikar1').from('{}');",&COLLECTION).as_str()).await;
    println!("{:?}",res);

    // Perform a search query in the collection
    let res = planner.exec(format!("search.query('julfikar 1').from('{}');",&COLLECTION).as_str()).await;
    println!("{:?}",res);

    // Perform a conditional search query in the collection
    let res = planner.exec(format!("search.when(:map(\"age\") == 0:).query('julfikar').from('{}');",&COLLECTION).as_str()).await;
    println!("{:?}",res);
}
```

These FLQL queries can be executed using the Flinch Query Planner and provide a flexible and efficient way to interact with Flinch collections.
