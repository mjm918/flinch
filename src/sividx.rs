
use std::collections::HashSet;
use std::hash::Hash;
use dashmap::{DashMap, DashSet};
use dashmap::mapref::one::Ref;

pub fn build_inverted_index<K: AsRef<str>>(map: &DashMap<K, String>) -> DashMap<String, DashSet<String>>
    where K: Hash, K: std::cmp::Eq{
    let inverted_index: DashMap<String, DashSet<String>> = DashMap::new();
    map.iter().for_each(|entry| {
        let (key, value) = entry.pair();
        let key_str = key.as_ref().to_string();
        for i in 0..key_str.len() {
            for j in i + 1..=key_str.len() {
                let substr = &key_str[i..j];
                inverted_index
                    .entry(substr.to_string())
                    .or_insert_with(DashSet::new)
                    .insert(key_str.clone());
            }
        }
    });
    inverted_index
}

// Function to search for keys in a DashMap by substring using the inverted index
pub fn search_contains<'a, K: AsRef<str>>(
    inverted_index: &'a DashMap<String, DashSet<String>>,
    substr: &str,
) -> Vec<Ref<'a, String, DashSet<String>>> {
    let substr = substr.to_string();
    let result = inverted_index
        .get(&substr)
        .map(|set| set.value().clone())
        .unwrap_or_else(DashSet::new);

    let keys: Vec<_> = result.into_iter().collect();
    let key_refs: Vec<Ref<'_, String, DashSet<String>>> = keys
        .iter()
        .map(|key| inverted_index.get(key).expect("Key not found"))
        .collect();
    key_refs
}