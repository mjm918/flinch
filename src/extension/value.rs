use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use crate::extension::{JsonExt, JsonMapExt};

impl JsonExt for Value {
	fn to_object(&self) -> Map<String, Value> {
		let tmp_map = Map::new();
		let map = self.as_object().unwrap_or(&tmp_map).clone();
		map
	}

	fn to_array(&self) -> Vec<Value> {
		let arr = self.as_array().unwrap_or(&vec![]).clone();
		arr
	}

	fn any_to_str(&self) -> String {
		match self.as_bool() {
			None => match self.is_string() {
				true => {
					let str = self.as_str().unwrap_or("").to_owned();
					str
				}
				false => self.to_string()
			}
			Some(in_bool) => match in_bool {
				true => format!("true"),
				false => format!("false")
			}
		}
	}

	fn to_struct<'a, T>(&self) -> serde_json::Result<T> where T : Serialize + DeserializeOwned {
		let res: serde_json::Result<T> = serde_json::from_value(self.clone());
		res
	}
}

impl JsonMapExt for Map<String, Value> {
	fn get_str(&self, name: &str) -> String {
		let tmp = Value::String("".to_string());
		let df = self.get(name).unwrap_or(&tmp);
		let string = df.as_str().unwrap_or("");
		string.to_string()
	}

	fn get_object(&self, name: &str) -> Map<String, Value> {
		let tmp_map = Map::new();
		let tmp = Value::Object(tmp_map.to_owned());
		let df = self.get(name).unwrap_or(&tmp);
		let obj = df.as_object().unwrap_or(&tmp_map).clone();
		obj
	}

	fn get_array(&self, name: &str) -> Vec<Value> {
		let tmp = Value::Array(vec![]);
		let df = self.get(name).unwrap_or(&tmp);
		let arr = df.as_array().unwrap_or(&vec![]).clone();
		arr
	}
}