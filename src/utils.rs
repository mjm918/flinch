use std::time::{Duration, Instant};

pub struct ExecTime {
    pub timer: Instant,
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

pub static TIMEOUT: Duration = Duration::from_secs(5);

pub fn set_view_name(name: &str) -> String {
    format!(":view:{}",name)
}
pub fn get_view_name(name: &str) -> String {
    name.replace(":view:","")
}