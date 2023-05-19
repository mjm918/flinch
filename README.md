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