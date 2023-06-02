use crate::headers::NotificationType;

pub const OPERATION_INSERT: &'static str = "Insert";
pub const OPERATION_DELETE: &'static str = "Remove";

impl<K, D> NotificationType<K, D> {
    pub fn from_raw(type_id: &'static str, key: K, doc: Option<D>) -> NotificationType<K, D> {
        match type_id {
            OPERATION_INSERT => NotificationType::Insert(key, doc.unwrap()),
            OPERATION_DELETE => NotificationType::Remove(key),
            _ => panic!("failed")
        }
    }

    pub fn into_raw(self) -> (&'static str, K, Option<D>) {
        match self {
            NotificationType::Insert(k, d) => (OPERATION_INSERT, k, Some(d)),
            NotificationType::Remove(k) => (OPERATION_DELETE, k, None),
        }
    }
}