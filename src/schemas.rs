use std::time::Instant;

use anyhow::anyhow;
use dashmap::DashMap;
use flql::Flql;

use crate::authenticate::Authenticate;
use crate::errors::DbError;
use crate::headers::{DbName, DbUser, FlinchCnf, FlinchError, QueryResult, SessionId};
use crate::persistent::Persistent;
use crate::query::Query;
use crate::utils::{cnf_content, database_path, DBLIST_PREFIX, DBUSER_PREFIX, ExecTime, uuid};

pub struct Schemas {
    pub dbs: DashMap<DbName, Query>,
    auth: Authenticate,
    internal: Persistent,
    config: FlinchCnf,
}

impl Schemas {
    pub async fn init() -> anyhow::Result<Self> {
        let cnf = cnf_content();
        if cnf.is_err() {
            return Err(anyhow!(cnf.err().unwrap()));
        }
        let cnf = cnf.unwrap();

        let db_dir = &cnf.data_dir;
        let db = sled::open(database_path(Some(db_dir.to_lowercase())));
        if db.is_err() {
            panic!("error opening flinch-internal storage {}", db.err().unwrap());
        }
        let db = db.unwrap();

        let slf = Self {
            dbs: DashMap::new(),
            auth: Authenticate::new(&db, &cnf),
            internal: Persistent::open(&db, "store"),
            config: cnf,
        };

        let docs = slf.internal.prefix(format!("{}", &DBLIST_PREFIX));
        for (_, db_name) in docs {
            slf.dbs.insert(db_name.to_owned(), Query::new_with_name(db_name.as_str()).await);
        }

        Ok(slf)
    }

    pub fn login(&self, user: &str, pw: &str, db: &str) -> anyhow::Result<SessionId, DbError> {
        self.auth.login(user, pw, db)
    }

    pub async fn flql(&self, stmt: &str, session_id: SessionId) -> QueryResult {
        let ttk = ExecTime::new();
        let user = self.auth.user(session_id.clone());
        if user.is_none() {
            return QueryResult {
                data: vec![],
                error: FlinchError::SchemaError(DbError::NoSession),
                time_taken: ttk.done(),
            };
        }
        let user = user.unwrap();

        let parsed = flql::parse(stmt);
        if parsed.is_err() {
            return QueryResult {
                data: vec![],
                error: FlinchError::CustomError(parsed.err().unwrap()),
                time_taken: ttk.done(),
            };
        }

        let db = self.dbs.get(user.db.as_str());
        if db.is_none() {
            return QueryResult {
                data: vec![],
                error: FlinchError::SchemaError(DbError::DbNotExists(user.clone().db)),
                time_taken: ttk.done(),
            };
        }
        let db = db.unwrap();
        let mut db = db.value();

        let parsed = parsed.unwrap();
        match parsed {
            Flql::DbNew(permit) => {
                if user.db.ne("*") {
                    return QueryResult {
                        data: vec![],
                        error: FlinchError::SchemaError(DbError::UserNoPermission),
                        time_taken: ttk.done(),
                    };
                }
                let permit = self.convert_permit(permit.as_str());
                if permit.is_err() {
                    return QueryResult {
                        data: vec![],
                        error: FlinchError::SchemaError(permit.err().unwrap()),
                        time_taken: ttk.done(),
                    };
                }
                let permit = permit.unwrap();

                let res = self.new(permit).await;
                QueryResult {
                    data: vec![],
                    error: match res {
                        Ok(_) => FlinchError::None,
                        Err(_) => FlinchError::SchemaError(res.err().unwrap()),
                    },
                    time_taken: ttk.done(),
                }
            }
            Flql::DbDrop(db) => {
                if user.db.ne("*") {
                    return QueryResult {
                        data: vec![],
                        error: FlinchError::SchemaError(DbError::UserNoPermission),
                        time_taken: ttk.done(),
                    };
                }
                let res = self.drop(db.as_str()).await;
                QueryResult {
                    data: vec![],
                    error: match res {
                        Ok(_) => FlinchError::None,
                        Err(_) => FlinchError::SchemaError(res.err().unwrap()),
                    },
                    time_taken: ttk.done(),
                }
            }
            Flql::DbPerm(permit) => {
                let res = self.new_user(permit.as_str());
                QueryResult {
                    data: vec![],
                    error: match res {
                        Ok(_) => FlinchError::None,
                        Err(_) => FlinchError::SchemaError(res.err().unwrap()),
                    },
                    time_taken: ttk.done(),
                }
            }
            _ => db.exec_with_flql(parsed).await
        }
    }

    async fn new(&self, permit: DbUser) -> anyhow::Result<(), DbError> {
        if self.dbs.contains_key(permit.name.as_str()) {
            return Err(DbError::DbExists(permit.name.clone()));
        }
        let db_name = permit.db.to_owned();

        let query = Query::new_with_name(db_name.as_str()).await;

        self.dbs.insert(db_name.to_owned(), query);
        self.internal.put_any(uuid(), format!("{}{}", &DBLIST_PREFIX, db_name));
        self._new_user(permit);

        Ok(())
    }

    fn new_user(&self, permit: &str) -> anyhow::Result<(), DbError> {
        let permit = self.convert_permit(permit);
        if permit.is_err() {
            return Err(permit.err().unwrap());
        }
        let permit = permit.unwrap();
        if !self.dbs.contains_key(permit.db.as_str()) {
            return Err(DbError::DbNotExists(permit.db));
        }
        self._new_user(permit);
        Ok(())
    }

    fn _new_user(&self, permit: DbUser) {
        self.auth.add_user_with_hash(permit.clone());
        self.internal.put_any(format!("{}{}", &DBUSER_PREFIX, uuid()), permit);
    }

    fn convert_permit(&self, permit: &str) -> anyhow::Result<DbUser, DbError> {
        let permit = serde_json::from_str::<DbUser>(permit);
        if permit.is_err() {
            return Err(DbError::InvalidPermissionConfig);
        }
        let permit = permit.unwrap();
        Ok(permit)
    }

    async fn drop(&self, name: &str) -> anyhow::Result<(), DbError> {
        if !self.dbs.contains_key(&name.to_string()) {
            return Err(DbError::DbNotExists(name.to_string()));
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
