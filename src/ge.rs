use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
use bincode::config;
use serde::{Deserialize,Serialize};
use uuid::Uuid;

pub struct Listener {
    callback: Arc<dyn Fn(Vec<u8>) + Sync + Send + 'static>,
    limit: Option<u64>,
    id: String,
}

#[derive(Default)]
pub struct EventEmitter {
    pub listeners: HashMap<String, Vec<Listener>>
}

impl EventEmitter {
    pub fn new() -> Self {
        Self {
            listeners: HashMap::new()
        }
    }

    pub fn on<F, T>(&mut self, event: &str, callback: F) -> String
        where
                for<'de> T: Deserialize<'de> + bincode::Decode,
                F: Fn(T) + 'static + Sync + Send
    {
        let id = self.on_limited(event, None, callback);
        return id;
    }

    pub fn emit<T>(&mut self, event: &str, value: T) -> Vec<thread::JoinHandle<()>>
        where T: Serialize + bincode::Encode
    {
        let mut callback_handlers: Vec<thread::JoinHandle<()>> = Vec::new();

        if let Some(listeners) = self.listeners.get_mut(event) {
            let bytes: Vec<u8> = bincode::encode_to_vec(&value,config::standard()).unwrap();

            let mut listeners_to_remove: Vec<usize> = Vec::new();
            for (index, listener) in listeners.iter_mut().enumerate() {
                let cloned_bytes = bytes.clone();
                let callback = Arc::clone(&listener.callback);

                match listener.limit {
                    None => {
                        callback_handlers.push(thread::spawn(move || {
                            callback(cloned_bytes);
                        }));
                    },
                    Some(limit) => {
                        if limit != 0 {
                            callback_handlers.push(thread::spawn(move || {
                                callback(cloned_bytes);
                            }));
                            listener.limit = Some(limit - 1);
                        } else {
                            listeners_to_remove.push(index);
                        }
                    }
                }
            }

            for index in listeners_to_remove.into_iter().rev() {
                listeners.remove(index);
            }
        }

        return callback_handlers;
    }

    pub fn remove_listener(&mut self, id_to_delete: &str) -> Option<String> {
        for (_,event_listeners) in self.listeners.iter_mut() {
            if let Some(index) = event_listeners.iter().position(|listener| listener.id == id_to_delete) {
                event_listeners.remove(index);
                return Some(id_to_delete.to_string());
            }
        }

        return None;
    }

    fn on_limited<F, T>(&mut self, event: &str, limit: Option<u64>, callback: F) -> String
        where
                for<'de> T: Deserialize<'de> + bincode::Decode,
                F: Fn(T) + 'static + Sync + Send
    {
        let id = Uuid::new_v4().to_string();
        let parsed_callback = move |bytes: Vec<u8>| {
            let value: T = bincode::decode_from_slice(&bytes,config::standard()).unwrap().0;
            callback(value);
        };

        let listener = Listener {
            id: id.clone(),
            limit,
            callback: Arc::new(parsed_callback),
        };

        match self.listeners.get_mut(event) {
            Some(callbacks) => { callbacks.push(listener); },
            None => { self.listeners.insert(event.to_string(), vec![listener]); }
        }

        return id;
    }

    pub fn once<F, T>(&mut self, event: &str, callback: F) -> String
        where
                for<'de> T: Deserialize<'de> + bincode::Decode,
                F: Fn(T) + 'static + Sync + Send
    {
        let id = self.on_limited(event, Some(1), callback);
        return id;
    }
}

lazy_static::lazy_static! {
    pub static ref EVENT_EMITTER: Mutex<EventEmitter> = Mutex::new(EventEmitter::new());
}