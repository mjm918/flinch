use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Bound;

use dashmap::{DashMap, DashSet};
use log::trace;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::doc_trait::Document;

pub struct Range<K> {
    pub tree: DashMap<String, BTreeMap<String, DashSet<K>>>,
}

impl<K> Range<K>
    where K: Serialize
    + DeserializeOwned
    + PartialOrd
    + Ord
    + PartialEq
    + Eq
    + Hash
    + Clone
    + Send
    + Debug
    + Display
    + 'static
{
    pub fn new() -> Self {
        Self {
            tree: DashMap::new()
        }
    }

    pub fn put<D>(&self, k: &K, v: &D) where D: Document {
        v.fields().into_iter().for_each(|f| {
            let val = f.value;
            match self.tree.get_mut(&f.key) {
                None => {
                    let mut tree = BTreeMap::new();
                    let set = DashSet::new();
                    set.insert(k.clone());
                    tree.insert(val, set);
                    self.tree.insert(f.key, tree);
                }
                Some(mut tree) => match tree.get_mut(&val) {
                    None => {
                        let set = DashSet::new();
                        set.insert(k.clone());
                        tree.value_mut().insert(val, set);
                    }
                    Some(set) => {
                        set.insert(k.clone());
                    }
                }
            }
        });
    }

    pub fn delete<D>(&self, k: &K, d: &D) where D: Document {
        trace!("deleting range for key - {}",&k);
        d.fields().into_iter().for_each(|f|{
            if let Some(mut tree) = self.tree.get_mut(&f.key) {
                if let Some(set) = tree.value_mut().get_mut(&f.value) {
                    set.insert(k.clone());
                }
            }
        });
    }

    pub fn delete_tree(&self, f: &str) {
        trace!("deleting range tree {}",&f);
        self.tree.remove(f);
    }

    pub fn clear_trees(&self) {
        let non_ref = self.tree.clone();
        for kv in non_ref {
            self.delete_tree(kv.0.as_str());
        }
    }

    pub fn range(&self, f: &str, from: String, to: String) -> Vec<K> {
        match self.tree.get(f) {
            None => BTreeSet::new(),
            Some(tree) => {
                let mut res = BTreeSet::new();
                for (_, set) in tree.range((Bound::Included(from), Bound::Included(to))) {
                    for el in set.iter() {
                        res.insert(el.key().clone());
                    }
                }
                res
            }
        }.into_iter().collect::<Vec<K>>()
    }
}
