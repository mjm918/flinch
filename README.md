<img src="assets/flinch.png">

[![Rust](https://github.com/mjm918/flinch/actions/workflows/rust.yml/badge.svg)](https://github.com/mjm918/flinch/actions/workflows/rust.yml)

Flinch is an in-memory, real-time document database designed for fast, efficient full-text search and querying. It comes with a built-in full-text search engine that enables both "search-as-you-type" and like search capabilities. Flinch was created with the goal of providing a high-performance search solution that can be integrated into various applications.

# Features
- In-memory database: Flinch stores documents in memory, allowing for ultra-fast search performance.
- Real-time updates: Flinch supports real-time updates, enabling users to add, update, and delete documents in real-time.
- Full-text search: Flinch has a built-in full-text search engine that provides powerful search capabilities, including search-as-you-type and wildcard search.
- Lightweight and easy to use: Flinch is designed to be lightweight and easy to use, with a simple API that allows developers to quickly integrate it into their applications.
- Document-oriented: Flinch is document-oriented, allowing users to store and retrieve documents as JSON objects.
- Highly scalable: Flinch is designed to be highly scalable, allowing users to handle large volumes of documents and queries efficiently.\
- Query: Document query faster than ⚡️
- Open source: Flinch is an open-source project, allowing users to contribute to its development and customize it to suit their needs.

# How to use

Refer to [lib.rs](src%2Flib.rs)

# Query Example

 ```
    CREATE collection -> {};

    DROP collection;
    
    LEN collection;
    
    UPSERT collection [{"avc":"1123"}];
    
    UPSERT collection {"avc":"1123"} WHERE $or:[{"$eq":{"a.b":1}}] $and:[{"$lt":{"a":3}}];
    
    PUT collection -> id -> {};
    
    EXISTS collection -> id;
    
    SEARCH collection -> 'your random query' OFFSET 0 LIMIT 1000000;
    
    GET collection WHERE {} SORT id DESC OFFSET 0 LIMIT 1000000;
    
    GET collection WHERE $or:[{"$eq":{"a.b":3}},{"$lt":{"b":3}}] OFFSET 0 LIMIT 1000000;
    
    GET collection -> id;
    
    DELETE collection -> id;
    
    DELETE collection WHERE $or:[{"$eq":{"a.b":1}}] $and:[{"$lt":{"a":3}}];
 ```