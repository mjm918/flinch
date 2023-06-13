use serde::{Deserialize, Serialize};

pub const FLINCH: &str = "flinch";
pub const INTERNAL_COL: &str = "__flinch__internal__";
pub const INTERNAL_TREE: &str = "__flinch__store__";

pub const MAGIC_DB: &str = "*";
pub const MIN_USERNAME_LEN: usize = 4;
pub const MAX_USERNAME_LEN: usize = 20;
pub const MIN_DBNAME_LEN: usize = 4;
pub const MAX_DBNAME_LEN: usize = 10;
pub const MIN_PW_LEN: usize = 4;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum PermissionTypes {
    AssignUser,
    CreateCollection,
    DropCollection,
    Read,
    Write,
    Flush,
}