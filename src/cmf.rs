use serde_json::Value;
use crate::hdrs::DataTypes;
use crate::kwr::Keywords;

pub struct CompareFactors {
    pub lt: Option<Value>,
    pub gt: Option<Value>,
    pub eq: Option<Value>,
    pub neq: Option<Value>,
    pub like: Option<Value>,
    pub prop: Option<String>,
    pub data_type: DataTypes,
}

impl CompareFactors {
    pub fn new() -> Self {
        Self {
            lt: None,
            gt: None,
            eq: None,
            neq: None,
            like: None,
            prop: None,
            data_type: DataTypes::String,
        }
    }
    pub fn set(&mut self, cmd: &str, v: Value) {
        if v.is_f64() || v.is_i64() {
            self.data_type = DataTypes::Number;
        } else {
            self.data_type = DataTypes::String;
        }
        match Keywords::from(cmd) {
            Keywords::Lt => { self.lt = Some(v); },
            Keywords::Gt => { self.gt = Some(v); },
            Keywords::Eq => { self.eq = Some(v); },
            Keywords::Neq => { self.neq = Some(v); },
            Keywords::Like => { self.like = Some(v); },
            Keywords::Prop => { self.prop = Some(v.to_string()); },
            _ => {}
        };
    }
}