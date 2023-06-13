use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use log::error;
use regex::Regex;
use uuid::Uuid;

use crate::headers::{FlinchCnf, FlinchCnfDir, FlinchCnfEnable, FlinchCnfLogin, Sort, SortDirection};
use crate::pri_headers::FLINCH;

pub struct ExecTime {
    timer: Instant,
}

impl ExecTime {
    pub fn new() -> Self {
        Self {
            timer: Instant::now()
        }
    }
    pub fn done(&self) -> String {
        format!("{:?}", self.timer.elapsed())
    }
}

pub static DBLIST_PREFIX: &str = ":db-list:";
pub static DBUSER_PREFIX: &str = ":db-user:";
pub static TTL_PREFIX: &str = ":ttl:";
pub static COL_PREFIX: &str = ":collection:";
pub static DOC_PREFIX: &str = ":document:";
pub static TIMEOUT: Duration = Duration::from_secs(5);

pub fn database_path(name: Option<String>) -> String {
    let mut db_name = "flinch".to_string();
    if name.is_some() {
        let name = name.unwrap();
        db_name = name.to_owned();
    }
    let cnf = cnf_content().unwrap();
    Path::new(".")
        .join(cnf.dir.data)
        .join(db_name)
        .to_str()
        .unwrap()
        .to_string()
}

pub fn set_view_name(name: &str) -> String {
    format!(":view:{}", name)
}

pub fn prefix_col_name(name: &str) -> String { format!("{}{}", COL_PREFIX, name) }

pub fn prefix_doc(k: &str) -> String { format!("{}{}", DOC_PREFIX, k) }

pub fn prefix_ttl(key: &str) -> String { format!("{}{}", TTL_PREFIX, key) }

pub fn get_col_name(name: &str) -> String { name.replace(COL_PREFIX, "") }

pub fn get_doc_name(name: &str) -> String { name.replace(DOC_PREFIX, "") }

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
            Some(Sort { field: trim_apos(&field.to_string()), direction })
        }
    }
}

pub fn parse_limit(opt: Option<String>) -> Option<(usize, usize)> {
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

fn cnf_path() -> String {
    Path::new(".")
        .join("flinch.toml")
        .to_str()
        .unwrap()
        .to_string()
}

pub fn cnf_content() -> anyhow::Result<FlinchCnf> {
    let path = cnf_path();
    if !Path::new(path.as_str()).exists() {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .append(true)
            .open(path.as_str())
            .unwrap();

        // write default config
        let login = FlinchCnfLogin { username: "root".to_string(), password: "flinch".to_string() };
        let enable = FlinchCnfEnable { log: true, mem_watch: true };
        let dir = FlinchCnfDir {
            data: "data".to_string(),
            log: "log".to_string(),
            mem_watch: "etc".to_string(),
        };
        let cnf = FlinchCnf {
            login,
            dir,
            enable,
        };
        let cnf = toml::to_string(&cnf);
        if cnf.is_err() {
            error!("error creating default config");
            return Err(anyhow!(cnf.err().unwrap()));
        }
        let content = cnf.unwrap();
        if let Err(err) = writeln!(file, "{}", content) {
            error!("error writing default config {}",err);
            return Err(anyhow!(err));
        }
    }
    let content = fs::read_to_string(path.as_str());
    if content.is_err() {
        panic!("failed to read config file");
    }
    let content = content.unwrap();
    let cnf = toml::from_str::<FlinchCnf>(content.as_str());
    if cnf.is_err() {
        panic!("failed to parse config file");
    }
    let cnf = cnf.unwrap();
    Ok(cnf)
}

pub fn uuid() -> String {
    Uuid::new_v4().as_hyphenated().to_string()
}

pub fn db_name_ok(name: &str) -> bool {
    if name.eq(FLINCH) {
        return false;
    }
    let windows_regex = Regex::new(r#"(?i)[\x00-\x1F\x7F<>:"/\\|?*]|[.]$|^ $|^\.|\.$|^(\.|\s)*$"#).unwrap();
    let linux_regex = Regex::new(r#"/$|^\.|\.$|^(\.|\s)*$"#).unwrap();

    !windows_regex.is_match(name) && !linux_regex.is_match(name)
}

pub fn get_log_filename() -> String {
    let date = chrono::Local::now();
    format!("{}{}.log", FLINCH, date.format("%Y%m%d_%H%M%S").to_string())
}

pub fn make_log_path(path: &str) -> String {
    Path::new(".")
        .join(path)
        .join(get_log_filename())
        .to_str()
        .unwrap()
        .to_lowercase()
}