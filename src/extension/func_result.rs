use serde_json::{Map, Value};
use crate::doc::QueryBased;
use crate::doc_trait::Document;
use crate::extension::FuncResultExtractor;
use crate::headers::FuncResult;

impl FuncResultExtractor for FuncResult<Option<(String, QueryBased)>> {
	fn get_object(&self) -> Map<String, Value> {
		if let Some((_key, qb)) = &self.data {
			return qb.object().clone();
		}
		Map::new()
	}
}