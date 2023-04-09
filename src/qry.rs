use crate::hdrs::Query;

pub const QUERY_INSERT: &'static str = "Insert";
pub const QUERY_REMOVE: &'static str = "Remove";

impl<K, D> Query<K, D> {
    pub fn from_raw(type_id: &'static str, key: K, doc: Option<D>) -> Query<K, D> {
        match type_id {
            QUERY_INSERT => Query::Insert(key, doc.unwrap()),
            QUERY_REMOVE => Query::Remove(key),
            _ => panic!("failed")
        }
    }

    pub fn into_raw(self) -> (&'static str, K, Option<D>) {
        match self {
            Query::Insert(k, d) => (QUERY_INSERT, k, Some(d)),
            Query::Remove(k) => (QUERY_REMOVE, k, None),
        }
    }
}