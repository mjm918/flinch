use crate::hdrs::ActionType;

pub const ACTION_INSERT: &'static str = "Insert";
pub const ACTION_REMOVE: &'static str = "Remove";

impl<K, D> ActionType<K, D> {
    pub fn from_raw(type_id: &'static str, key: K, doc: Option<D>) -> ActionType<K, D> {
        match type_id {
            ACTION_INSERT => ActionType::Insert(key, doc.unwrap()),
            ACTION_REMOVE => ActionType::Remove(key),
            _ => panic!("failed")
        }
    }

    pub fn into_raw(self) -> (&'static str, K, Option<D>) {
        match self {
            ActionType::Insert(k, d) => (ACTION_INSERT, k, Some(d)),
            ActionType::Remove(k) => (ACTION_REMOVE, k, None),
        }
    }
}