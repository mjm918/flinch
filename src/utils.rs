use std::path::Path;
use std::time::{Duration, Instant};
use crate::hdrs::{Sort, SortDirection};

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
pub static TTL_PREFIX: &str = ":ttl:";
pub static COL_PREFIX: &str = ":collection:";
pub static DOC_PREFIX: &str = ":document:";
pub static TIMEOUT: Duration = Duration::from_secs(5);

pub fn database_path() -> String { Path::new(".").join("dbs").to_str().unwrap().to_string() }

pub fn set_view_name(name: &str) -> String {
    format!(":view:{}",name)
}

pub fn prefix_col_name(name: &str) -> String { format!("{}{}", COL_PREFIX, name) }

pub fn prefix_doc(k: &str) -> String { format!("{}{}",DOC_PREFIX, k) }

pub fn prefix_ttl(key: &str) -> String { format!("{}{}", TTL_PREFIX, key) }

pub fn get_col_name(name: &str) -> String { name.replace(COL_PREFIX,"") }

pub fn get_doc_name(name: &str) -> String { name.replace(DOC_PREFIX,"") }

pub fn get_ttl_name(name: &str) -> String { name.replace(TTL_PREFIX, "") }

pub fn tokenize(query: &String) -> Vec<String> {
    query
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

pub fn trim_apos(name: &String) -> String {
    name.trim_matches('\'').to_string()
}

pub fn parse_sort(opt: Option<String>) -> Option<Sort> {
    match opt {
        None => {
            None
        }
        Some(sort) => {
            let tkns = sort.split(',').collect::<Vec<&str>>();
            let field = tkns.get(0).unwrap().to_owned();
            let direction = tkns.get(1).unwrap().to_owned();
            let direction = match direction {
                "ASC" => SortDirection::Asc,
                "DESC" => SortDirection::Desc,
                _ => SortDirection::Asc
            };
            Some(Sort{ field: trim_apos(&field.to_string()), direction })
        }
    }
}

pub fn parse_limit(opt: Option<String>) -> Option<(usize,usize)> {
    match opt {
        None => { None }
        Some(ol) => {
            let tkns = ol.split(',').collect::<Vec<&str>>();
            let offset = tkns.get(0).unwrap().to_owned();
            let offset = offset.parse::<usize>().unwrap();
            let limit = tkns.get(1).unwrap().to_owned();
            let limit = limit.parse::<usize>().unwrap();
            Some((offset, limit))
        }
    }
}