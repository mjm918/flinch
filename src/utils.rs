use std::time::{Duration, Instant};

pub struct ExecTime {
    timer: Instant,
    first_hit: String,
}

impl ExecTime {
    pub fn new() -> Self {
        Self {
            timer: Instant::now(),
            first_hit: "".to_string()
        }
    }
    pub fn done(&self) -> String {
        if self.first_hit.is_empty() {
            format!("{:?}", self.timer.elapsed())
        } else {
            format!("first hit {} time taken {:?}", self.first_hit, self.timer.elapsed())
        }
    }
}

pub static TIMEOUT: Duration = Duration::from_secs(5);

pub fn set_view_name(name: &str) -> String {
    format!(":view:{}",name)
}
pub fn get_view_name(name: &str) -> String {
    name.replace(":view:","")
}
pub fn tokenize(query: &String) -> Vec<String> {
    query
        .clone()
        .trim()
        .to_lowercase()
        .replace("(", " ")
        .replace(")", " ")
        .replace("+", " ")
        .replace("-", " ")
        .replace("/", " ")
        .replace("\\", " ")
        .replace("_", " ")
        .replace("[", " ")
        .replace("]", " ")
        .split_whitespace()
        .into_iter()
        .filter(|&char| !char.is_empty())
        .map(|char| char.to_string())
        .collect::<Vec<_>>()
}