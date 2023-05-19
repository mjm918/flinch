use anyhow::Result;
use std::sync::Arc;
use evalexpr::*;
use serde_json::{Value as SerdeValue, from_str as JsonFromStr, to_string as StrToJson};
use crate::db::{CollectionOptions, Database};
use crate::docv::QueryBased;
use crate::err::QueryError;
use crate::qry::Query;

impl Query {
    pub fn data_context_filter(json: &SerdeValue) -> HashMapContext {
        let data_map = json.clone();
        let data_array_map = json.clone();
        let data_array_filter = json.clone();
        let context = context_map! {
            "map" => Function::new(move |args|{
                if let Ok(first) = args.as_string() {
                    return Ok(Self::map(&data_map, &first));
                }
                Err(EvalexprError::type_error(args.clone(),vec![ValueType::String, ValueType::Tuple]))
            }),
            "includes" => Function::new(|args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    if let Ok(first) = &arguments[0].as_string() {
                        let array = JsonFromStr(first.as_str());
                        if array.is_ok() {
                            let array: Vec<serde_json::Value> = array.unwrap();
                            if Self::includes(&array, &arguments[1]) {
                                return Ok(Value::Boolean(true));
                            }
                        } else {
                            return Err(EvalexprError::type_error(Value::String(first.clone()), vec![ValueType::Tuple]));
                        }
                    } else {
                        return Err(EvalexprError::type_error(Value::String(arguments[0].to_string()), vec![ValueType::Tuple]));
                    }
                    return Err(EvalexprError::CustomMessage("item does not exists".to_owned()));
                }
                Err(EvalexprError::type_error(args.clone(),vec![ValueType::Tuple, ValueType::String, ValueType::Boolean, ValueType::Int, ValueType::Float]))
            }),
            "array_filter" => Function::new(move |args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    if let Ok(first) = &arguments[0].as_string() {
                        let pointers = format!("/{}",first.replace(".","/"));
                        let pointers = pointers.split("$").collect::<Vec<&str>>();
                        let pointer_value = Self::array_map(&data_array_filter, pointers);
                        let res = Self::includes(&pointer_value, &arguments[1]);
                        return Ok(Value::Boolean(res));
                    } else {
                        return Err(EvalexprError::type_error(Value::String(arguments[0].to_string()), vec![ValueType::String]));
                    }
                }
                Err(EvalexprError::type_error(args.clone(),vec![ValueType::Tuple, ValueType::String, ValueType::Boolean, ValueType::Int, ValueType::Float]))
            }),
            "array_map" => Function::new(move |args|{
                if let Ok(first) = &args.as_string() {
                    let pointers = format!("/{}",first.replace(".","/"));
                    let pointers = pointers.split("$").collect::<Vec<&str>>();
                    let pointer_value = Self::array_map(&data_array_map, pointers);
                    return Ok(Value::String(serde_json::to_string(&pointer_value).unwrap()));
                } else {
                    return Err(EvalexprError::type_error(Value::String(args.to_string()), vec![ValueType::String]));
                }
                Err(EvalexprError::type_error(args.clone(),vec![ValueType::String]))
            })
        }.unwrap();

        context
    }

    fn array_map(json: &SerdeValue, mut paths: Vec<&str>) -> Vec<SerdeValue> {
        if let Some(array) = json.as_array() {
            let mut values = vec![];
            for object in array {
                if let Some(current) = object.pointer(paths[0]) {
                    values.push(current.clone());
                }
            }
            return values;
        } else {
            if let Some(current) = json.pointer(paths[0]) {
                paths.remove(0);
                return Self::array_map(current, paths);
            }
        }
        return vec![];
    }
    fn map(json: &SerdeValue, subject: &String) -> Value {
        let pointer = format!("/{}",subject.replace(".","/"));
        if let Some(v) = json.pointer(pointer.as_str()) {
            if let Some(str) = v.as_str() {
                return Value::from(str);
            }
            if let Some(num) = v.as_f64() {
                return Value::from(num);
            }
            if let Some(num) = v.as_i64() {
                return Value::from(num);
            }
            if let Some(varr) = v.as_array() {
                return Value::from(serde_json::to_string(varr).unwrap());
            }
        }
        Value::Empty
    }
    fn includes(array: &Vec<SerdeValue>, second: &Value) -> bool {
        for item in array {
            if second.is_string() {
                if let Ok(rhs) = second.as_string() {
                    if let Some(lhs) = item.as_str() {
                        if rhs.as_str().eq(lhs) {
                            return true;
                        }
                    }
                }
            }
            if let Ok(rhs) = second.as_float() {
                if let Some(lhs) = item.as_f64() {
                    if rhs.eq(&lhs) {
                        return true;
                    }
                }
            }
            if let Ok(rhs) = second.as_int() {
                if let Some(lhs) = item.as_i64() {
                    if rhs.eq(&lhs) {
                        return true;
                    }
                }
            }
            if let Ok(rhs) = second.as_boolean() {
                if let Some(lhs) = item.as_bool() {
                    if rhs.eq(&lhs) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}