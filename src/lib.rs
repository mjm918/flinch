mod hidx;
mod ividx;
mod range;
mod clips;
mod err;
mod hdrs;
mod wtch;
mod sess;
mod act;
pub mod doc;
pub mod col;
pub mod db;
mod qry;
mod utils;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Instant};
    use lazy_static::lazy_static;
    use serde::{Deserialize, Serialize};
    use tokio::sync::mpsc::channel;
    use crate::doc::{Document, FromRawString, ViewConfig};
    use crate::db::{Database, CollectionOptions};
    use crate::hdrs::{Event, ActionType};

    lazy_static! {
        pub static ref DB: Database<String, FromRawString> = Database::init();
    }
    #[tokio::test]
    async fn it_works() {

    }
}
