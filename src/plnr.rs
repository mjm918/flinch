use std::time::Instant;
use evalexpr::*;
use rayon::prelude::*;
use serde_json::{Value as SerdeValue, from_str as JsonFromStr, to_string as StrToJson};
use dashmap::mapref::one::Ref;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use crate::col::Collection;
use crate::db::{CollectionOptions, Database};
use crate::doc::Document;
use crate::docv::QueryBased;
use crate::err::QueryError;
use crate::hdrs::{ActionResult, FuncResult};

pub struct Planner {
    db: Database<String, QueryBased>,
    context: HashMapContext
}

impl Planner {
    pub fn new(db: Database<String, QueryBased>) -> Self {
        let mut context = HashMapContext::new();
        Self { db, context: HashMapContext::new() }
    }

    pub fn exec<'a>(&'a self, stmt: &'a str) -> EvalexprResult<Value> {
        let binding = self.action_context_filter();
        let context = binding.as_ref().unwrap();
        eval_with_context(stmt, context)
    }

    fn col(&self, name:&str) -> Result<Ref<String, Collection<String, QueryBased>>, QueryError> {
        let col = self.db.using(name.clone());
        if col.is_ok() {
            Ok(col.unwrap())
        } else {
            Err(QueryError::CollectionNotExists(name.to_string()))
        }
    }

    fn action_context_filter(&self) -> Result<HashMapContext, EvalexprError> {
        context_map! {
            "new" => Function::new(|args|{
                let chk = self.check_new_col(args);
                if let Ok(options) = chk {
                    if let Err(er) = self.db.add(options) {
                        return Err(self.error_resp(format!("{}",er.to_string())));
                    }
                } else {
                    return Err(self.error_resp(chk.err().unwrap().to_string()));
                }
                Ok(().into())
            }),
            "drop" => Function::new(|args| {
                let chk = self.check_col_name(args, 1);
                let x = async {
                    if let Ok(arguments) = chk {
                        if let Err(er) = self.db.drop(arguments[0].as_str()).await {
                            return Err(self.error_resp(format!("{}",er.to_string())));
                        }
                    } else {
                        return Err(self.error_resp(chk.err().unwrap().to_string()));
                    }
                    Ok(().into())
                };
                futures::executor::block_on(x)
            }),
            "upsert" => Function::new(|args|{
                let chk = self.check_col_name(args, 2);
                let x = async {
                    if let Ok(arguments) = chk {
                        if let Ok(json) = QueryBased::from_str(arguments[1].as_str()) {
                            if let Ok(collection) = self.col(arguments[0].as_str()) {
                                let id = collection.id();
                                if let Ok(ttk) = collection.put(id.clone(), json).await {
                                    return Ok(self.action_resp(format!("success!") ,ttk));
                                } else {
                                    return Err(self.error_resp(format!("failed to insert")));
                                }
                            } else {
                                return Err(self.error_resp(format!("collection not found")));
                            }
                        } else {
                            return Err(self.error_resp(format!("malformed data")));
                        }
                    } else {
                        return Err(self.error_resp(format!("{}",chk.err().unwrap().to_string())));
                    }
                };
                futures::executor::block_on(x)
            }),
            "upsertWhere" => Function::new(|args|{
                let chk = self.check_col_name(args, 2);
                let (sx, rx) = futures::channel::mpsc::unbounded();
                let x = async {
                    if let Ok(arguments) = chk {
                        if let Ok(json) = QueryBased::from_str(arguments[1].as_str()) {
                            if let Ok(collection) = self.col(arguments[0].as_str()) {
                                collection.iter().for_each(|kv| {
                                    futures::executor::block_on(async {
                                        if let Ok(_) = eval_with_context(arguments[1].as_str(),&self.data_context_filter(&kv.data)) {
                                            let key = kv.key();
                                            if let Err(err) = collection.put(key.clone(), json.clone()).await {
                                                sx.unbounded_send(key.to_string()).expect("failed to send");
                                            }
                                        }
                                    });
                                });
                            }
                        }
                    } else {
                        return Err(self.error_resp(format!("{}",chk.err().unwrap().to_string())));
                    }
                    let v = rx.collect::<String>();

                    Ok(().into())
                };
                futures::executor::block_on(x)
            }),
            "putPointer" => Function::new(|args|{
                let chk = self.check_col_name(args, 1);
                let x = async {
                    if let Ok(arguments) = chk {

                    } else {
                        return Err(chk.err().unwrap());
                    }
                    Ok(().into())
                };
                futures::executor::block_on(x)
            }),
            "delete" => Function::new(|args|{
                let chk = self.check_col_name(args, 1);
                let x = async {
                    if let Ok(arguments) = chk {

                    } else {
                        return Err(chk.err().unwrap());
                    }
                    Ok(().into())
                };
                futures::executor::block_on(x)
            }),
            "deleteWhere" => Function::new(|args|{
                let chk = self.check_col_name(args, 1);
                let x = async {
                    if let Ok(arguments) = chk {

                    } else {
                        return Err(chk.err().unwrap());
                    }
                    Ok(().into())
                };
                futures::executor::block_on(x)
            }),
            "deletePointer" => Function::new(|args|{
                let chk = self.check_col_name(args, 1);
                let x = async {
                    if let Ok(arguments) = chk {

                    } else {
                        return Err(chk.err().unwrap());
                    }
                    Ok(().into())
                };
                futures::executor::block_on(x)
            }),
            "deleteView" => Function::new(|args|{
                let chk = self.check_col_name(args, 1);
                let x = async {
                    if let Ok(arguments) = chk {

                    } else {
                        return Err(chk.err().unwrap());
                    }
                    Ok(().into())
                };
                futures::executor::block_on(x)
            }),
            "deleteClip" => Function::new(|args|{
                let chk = self.check_col_name(args, 1);
                let x = async {
                    if let Ok(arguments) = chk {

                    } else {
                        return Err(chk.err().unwrap());
                    }
                    Ok(().into())
                };
                futures::executor::block_on(x)
            }),
            "search" => Function::new(|args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    println!("{:?}",arguments);
                }
                Ok(().into())
            }),
            "get" => Function::new(|args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    println!("{:?}",arguments);
                }
                Ok(().into())
            }),
            "getWhere" => Function::new(|args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    println!("{:?}",arguments);
                }
                Ok(().into())
            }),
            "getPointer" => Function::new(|args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    println!("{:?}",arguments);
                }
                Ok(().into())
            }),
            "getView" => Function::new(|args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    println!("{:?}",arguments);
                }
                Ok(().into())
            }),
            "getClip" => Function::new(|args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    println!("{:?}",arguments);
                }
                Ok(().into())
            }),
            "searchWhere" => Function::new(|args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    println!("{:?}",arguments);
                }
                Ok(().into())
            })
        }
    }

    fn data_context_filter(&self, json: &SerdeValue) -> HashMapContext {
        let data_map = json.clone();
        let data_array_map = json.clone();
        let data_array_filter = json.clone();
        let context = context_map! {
            "map" => Function::new(move |args|{
                if let Ok(first) = args.as_string() {
                    return Ok(self.map(&data_map, &first));
                }
                Err(EvalexprError::type_error(args.clone(),vec![ValueType::String, ValueType::Tuple]))
            }),
            "includes" => Function::new(|args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    if let Ok(first) = &arguments[0].as_string() {
                        let array = JsonFromStr(first.as_str());
                        if array.is_ok() {
                            let array: Vec<serde_json::Value> = array.unwrap();
                            if self.includes(&array, &arguments[1]) {
                                return Ok(Value::Boolean(true));
                            }
                        } else {
                            return Err(EvalexprError::type_error(Value::String(first.clone()), vec![ValueType::Tuple]));
                        }
                    } else {
                        return Err(EvalexprError::type_error(Value::String(arguments[0].to_string()), vec![ValueType::Tuple]));
                    }
                    return Ok(Value::Boolean(false));
                }
                Err(EvalexprError::type_error(args.clone(),vec![ValueType::Tuple, ValueType::String, ValueType::Boolean, ValueType::Int, ValueType::Float]))
            }),
            "array_filter" => Function::new(move |args|{
                if let Ok(arguments) = args.as_fixed_len_tuple(2) {
                    if let Ok(first) = &arguments[0].as_string() {
                        let pointers = format!("/{}",first.replace(".","/"));
                        let pointers = pointers.split("$").collect::<Vec<&str>>();
                        let pointer_value = self.array_map(&data_array_filter, pointers);
                        let res = self.includes(&pointer_value, &arguments[1]);
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
                    let pointer_value = self.array_map(&data_array_map, pointers);
                    return Ok(Value::String(serde_json::to_string(&pointer_value).unwrap()));
                } else {
                    return Err(EvalexprError::type_error(Value::String(args.to_string()), vec![ValueType::String]));
                }
                Err(EvalexprError::type_error(args.clone(),vec![ValueType::String]))
            })
        }.unwrap();

        context
    }

    fn array_map(&self, json: &SerdeValue, mut paths: Vec<&str>) -> Vec<SerdeValue> {
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
                return self.array_map(current, paths);
            }
        }
        return vec![];
    }
    fn map(&self, json: &SerdeValue, subject: &String) -> Value {
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
    fn includes(&self, array: &Vec<SerdeValue>, second: &Value) -> bool {
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

    fn check_new_col(&self, args: &Value) -> Result<CollectionOptions, EvalexprError> {
        return if let Ok(arguments) = args.as_fixed_len_tuple(1) {
            if let Ok(options) = arguments[0].as_string() {
                if let Ok(opts) = JsonFromStr::<CollectionOptions>(options.as_str()) {
                    Ok(opts)
                } else {
                    Err(EvalexprError::CustomMessage(format!("failed to parse collection options")))
                }
            } else {
                Err(EvalexprError::CustomMessage(format!("options cannot be converted CollectionOptions")))
            }
        } else {
            Err(EvalexprError::TypeError { actual: args.clone(), expected: vec![ValueType::String, ValueType::String] })
        }
    }

    fn check_col_name(&self, args: &Value, num_of_args: usize) -> Result<Vec<String>, EvalexprError> {
        return if let Ok(arguments) = args.as_fixed_len_tuple(num_of_args) {
            if let Ok(_) = arguments[0].as_string() {
                let mut rest = vec![];
                for argument in arguments {
                    rest.push(argument.as_string().unwrap());
                }
                Ok(rest)
            } else {
                Err(EvalexprError::CustomMessage(format!("name must be a string")))
            }
        } else {
            Err(EvalexprError::TypeError { actual: args.clone(), expected: vec![ValueType::String] })
        }
    }

    fn action_resp(&self, message: String, time_taken: String) -> Value {
        Value::String(StrToJson(&ActionResult{ error: false, message, time_taken }).unwrap())
    }

    fn error_resp(&self, message: String) -> EvalexprError {
        EvalexprError::CustomMessage(message)
    }

    fn query_resp<T>(&self, res: FuncResult<T>) -> Value where for<'a> T: Serialize + Deserialize<'a> {
        Value::String(StrToJson(&res).unwrap())
    }
}