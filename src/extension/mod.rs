pub mod flinch_ext;
mod func_result;
pub mod value;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

pub trait JsonExt {
	fn to_object(&self) -> Map<String, Value>;
	fn to_array(&self) -> Vec<Value>;
	fn any_to_str(&self) -> String;
	fn to_struct<'a, T>(&self) -> serde_json::Result<T> where T : Serialize + DeserializeOwned;
}

pub trait JsonMapExt {
	fn get_str(&self, name: &str) -> String;
	fn get_object(&self, name: &str) -> Map<String, Value>;
	fn get_array(&self, name: &str) -> Vec<Value>;
}

pub trait FuncResultExtractor {
	fn get_object(&self) -> Map<String, Value>;
}

#[async_trait]
pub trait FlinchDbHelper {
	async fn push<'a, T>(&self, col: &'a str, value: T) where T: Serialize + Deserialize<'a> + Sync + Send + ToString;
	async fn save<'a, T>(&self, col: &'a str, key: &'a str, value: T) where T: Serialize + Deserialize<'a> + Sync + Send + ToString;
	async fn save_with_ttl<'a, T>(&self, col: &'a str, key: &'a str, value: T, ttl: i64) where T: Serialize + Deserialize<'a> + Sync + Send + ToString;
	fn get_object(&self, col: &str, key: &str) -> Map<String, Value>;
	fn find_all<T>(&self, col: &str) -> Option<Vec<T>> where T: From<Map<String, Value>> ;
	fn find_one<T>(&self, col: &str, index: &str) -> Option<T> where T: From<Map<String, Value>> ;
	fn find_id_by_index(&self, col: &str, index: &str) -> Option<String>;
	async fn delete_id(&self, col: &str, id: &str) -> bool;
	async fn update_by_index<'a, T>(&self, col: &'a str, index: &'a str, value: T) -> bool where T: Serialize + Deserialize<'a> + Sync + Send + ToString;
}

pub trait ResultSet {
	fn result_array(self) -> Vec<Value>;
}
