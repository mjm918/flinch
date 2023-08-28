use std::sync::Arc;
use async_trait::async_trait;
use log::{error, trace};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use crate::database::Database;
use crate::doc::QueryBased;
use crate::doc_trait::Document;
use crate::extension::FlinchDbHelper;

#[async_trait]
impl FlinchDbHelper for Arc<Database<QueryBased>> {
	async fn push<'a, T>(&self, col: &'a str, value: T) where T: Serialize + Deserialize<'a> + Sync + Send + ToString {
		if let Ok(col) = self.using(col) {
			let document = QueryBased::from_str(value.to_string().as_str()).unwrap();
			let _ = col.put(col.id(), document).await;
		} else {
			error!("no collection `{}` found in flinch",col);
		}
	}

	async fn save<'a, T>(&self, col: &'a str, key: &'a str, value: T) where T: Serialize + Deserialize<'a> + Sync + Send + ToString {
		if let Ok(col) = self.using(col) {
			let document = QueryBased::from_str(value.to_string().as_str()).unwrap();
			let _ = col.put(format!("{}", key), document).await;
		} else {
			error!("no collection `{}` found in flinch",col);
		}
	}

	async fn save_with_ttl<'a, T>(&self, col: &'a str, key: &'a str, value: T, ttl: i64) where T: Serialize + Deserialize<'a> + Sync + Send + ToString {
		if let Ok(col) = self.using(col) {
			let document = QueryBased::from_str(value.to_string().as_str()).unwrap();
			let _ = col.put(format!("{}", key), document).await;
			let _ = col.put_ttl(format!("{}", key), ttl).await;
		} else {
			error!("no collection `{}` found in flinch",col);
		}
	}

	fn get_object(&self, col: &str, key: &str) -> Map<String, Value> {
		if let Ok(col) = self.using(col) {
			let qb = col.get(&format!("{}", key));
			return qb.get_object();
		} else {
			error!("no collection `{}` found in flinch",col);
		}
		Map::new()
	}

	fn find_all<T>(&self, col: &str) -> Option<Vec<T>> where T: From<Map<String, Value>> {
		match self.using(col) {
			Ok(col) => {
				let all = crossbeam_queue::SegQueue::new();
				col.iter().for_each(|kv|{
					let v = kv.value();
					trace!("{:?}",v.object());
					all.push(v.object().clone());
				});
				let res = all.into_iter().map(|item|T::from(item)).collect::<Vec<T>>();
				Some(res)
			},
			Err(err) => {
				error!("{:?}",err);
				None
			},
		}
	}

	fn find_one<T>(&self, col: &str, index: &str) -> Option<T> where T: From<Map<String, Value>>  {
		match self.using(col) {
			Ok(col) => {
				let res = col.get_index(index);
				if res.data.is_some() {
					return Some(T::from(res.get_object()));
				}
				None
			}
			Err(err) => {
				error!("{:?}",err);
				None
			},
		}
	}

	fn find_id_by_index(&self, col: &str, index: &str) -> Option<String> {
		match self.using(col) {
			Ok(col) => {
				let res = col.get_index(index);
				if res.data.is_some() {
					let id = res.data.unwrap();
					return Some(id.0);
				}
				None
			}
			Err(err) => {
				error!("{:?}",err);
				None
			}
		}
	}

	async fn delete_id(&self, col: &str, id: &str) -> bool {
		match self.using(col) {
			Ok(col) => {
				col.delete(format!("{}",id)).await;
				true
			}
			Err(_) => false
		}
	}

	async fn update_by_index<'a, T>(&self, col: &'a str, index: &'a str, value: T) -> bool where T: Serialize + Deserialize<'a> + Sync + Send + ToString {
		if let Ok(db) = self.using(col) {
			return match self.find_id_by_index(col,index) {
				None => false,
				Some(id) => {
					let key = format!("{}",id);
					let document = QueryBased::from_str(value.to_string().as_str()).unwrap();
					match db.put(key.to_owned(), document).await {
						Ok(_) => true,
						Err(err) => {
							error!("{}",err);
							false
						}
					}
				}
			};
		}
		false
	}
}