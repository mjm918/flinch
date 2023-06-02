use anyhow::anyhow;
use dashmap::DashMap;
use dashmap::mapref::one::RefMut;
use log::warn;
use serde::{Deserialize, Serialize};
use crate::persistent::Persistent;
use crate::err::DbError;
use crate::query::Query;
use crate::utils::{database_path, DBLIST_PREFIX, DBUSER_PREFIX, uuid};

pub type DbName = String;
pub type SessionId = String;
pub type UserName = String;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DbUser {
    pub db: Option<DbName>,
    pub user: String,
    pub pw: String,
    pub create: bool,
    pub drop: bool,
    pub read: bool,
    pub write: bool,
    pub permit: bool,
    pub flush: bool
}

pub struct Schemas {
    pub dbs: DashMap<DbName, Query>,
    pub creds: DashMap<DbName, DashMap<UserName, DbUser>>,
    pub sess: DashMap<SessionId, DbUser>,
    internal: Persistent,
}

impl Schemas {
    pub async fn init() -> Self {
        let db = sled::open(database_path(Some(format!("flinch-internal"))));
        if db.is_err() {
            panic!("error opening flinch-internal storage {}", db.err().unwrap());
        }
        let db = db.unwrap();
        let slf = Self {
            dbs: DashMap::new(),
            creds: DashMap::new(),
            sess: DashMap::new(),
            internal: Persistent::open(&db, "store")
        };
        let docs = slf.internal.prefix(format!("{}",&DBLIST_PREFIX));
        for (_, db_name) in docs {
            slf.dbs.insert(db_name.to_owned(), Query::new_with_name(db_name.as_str()).await);
        }
        let users = slf.internal.prefix(format!("{}",&DBUSER_PREFIX));
        for (_, user) in users {
            let json = serde_json::from_str::<DbUser>(user.as_str());
            if json.is_err() {
                warn!("user info from persistent storage failed to parse {}", user);
                continue;
            }
            let info = json.unwrap();
            let db_name = info.db.as_ref().unwrap();
            if let Some(creds) = slf.creds.get_mut(db_name.as_str()) {
                let user_map = creds.value();
                user_map.insert(info.user.to_owned(), info);
            }
        }

        slf
    }

    async fn new(&self, name: &str, permit: &str) -> anyhow::Result<()> {
        if self.dbs.contains_key(&name.to_string()) {
            return Err(anyhow!(DbError::DbExists(name.to_string())));
        }
        let permit = serde_json::from_str::<DbUser>(permit);
        if permit.is_err() {
            return Err(anyhow!(DbError::InvalidPermissionConfig));
        }
        let db_name = name.to_string();
        let mut permit = permit.unwrap();
        permit.db = Some(db_name.to_owned());

        let mut cred = DashMap::new();
        cred.insert(permit.user.to_owned(), permit.to_owned());

        self.creds.insert(db_name.to_owned(), cred);
        self.internal.put_any(format!("{}{}",&DBUSER_PREFIX,uuid()), permit);

        let query = Query::new_with_name(db_name.as_str()).await;
        self.dbs.insert(db_name.to_owned(),query);
        self.internal.put_any(uuid(), format!("{}{}",&DBLIST_PREFIX, db_name));

        Ok(())
    }

    async fn drop(&self, name: &str) -> anyhow::Result<()> {
        if !self.dbs.contains_key(&name.to_string()) {
            return Err(anyhow!(DbError::DbNotExists(name.to_string())));
        }
        let query = self.dbs.get(name).unwrap();
        let query = query.value();
        let db = query.underlying_db();
        let ls = db.ls();
        for col in ls {
            db.drop(col.as_str()).await.expect("collection to drop");
        }
        self.dbs.remove(name);
        Ok(())
    }
}
