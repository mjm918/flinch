#![feature(integer_atomics, const_fn_trait_bound)]

use std::alloc::System;
use std::fs::File;

use anyhow::anyhow;
use dashmap::DashMap;
use flql::Flql;
use log::{error, LevelFilter, trace};
use simplelog::{ColorChoice, CombinedLogger, Config, TerminalMode, TermLogger, WriteLogger};
use size::{Base, Size};
use sled::Db;

use crate::authenticate::Authenticate;
use crate::errors::DbError;
use crate::headers::{DbName, DbUser, FlinchCnf, FlinchError, QueryResult, SessionId};
use crate::persistent::Persistent;
use crate::pri_headers::{FLINCH, INTERNAL_TREE, MAGIC_DB, MAX_DBNAME_LEN, MAX_USERNAME_LEN, MIN_DBNAME_LEN, MIN_PW_LEN, MIN_USERNAME_LEN, PermissionTypes};
use crate::query::Query;
use crate::utils::{cnf_content, database_path, db_name_ok, DBLIST_PREFIX, DBUSER_PREFIX, ExecTime, make_log_path, trim_apos, uuid};
use crate::zalloc::Zalloc;

#[global_allocator]
static ALLOCMEASURE: Zalloc<System> = Zalloc::new(System);

pub struct Schemas {
    dbs: DashMap<DbName, Query>,
    auth: Authenticate,
    internal: Persistent,
    config: FlinchCnf,
    storage: Db,
}

impl Schemas {
    pub async fn init() -> anyhow::Result<Self> {
        let cnf = cnf_content();
        if cnf.is_err() {
            return Err(anyhow!(cnf.err().unwrap()));
        }
        let cnf = cnf.unwrap();
        if cnf.enable.log {
            let dir_create = std::fs::create_dir_all(cnf.dir.log.as_str());
            if dir_create.is_err() {
                panic!("failed to create log dir");
            }

            let writer = File::create(make_log_path(cnf.dir.log.as_str()));
            if writer.is_err() {
                panic!("log writer failed with error {:?}", writer.err().unwrap());
            }
            let writer = writer.unwrap();
            CombinedLogger::init(
                vec![
                    TermLogger::new(LevelFilter::Info, Config::default(), TerminalMode::Mixed, ColorChoice::Auto),
                    WriteLogger::new(LevelFilter::Error, Config::default(), writer),
                ]
            ).unwrap();
        }

        let db = sled::open(database_path(Some(FLINCH.to_string())));
        if db.is_err() {
            panic!("error opening flinch-internal storage {}", db.err().unwrap());
        }
        let db = db.unwrap();

        let slf = Self {
            dbs: DashMap::new(),
            auth: Authenticate::new(&db, &cnf),
            internal: Persistent::open(&db, INTERNAL_TREE),
            config: cnf.clone(),
            storage: db,
        };
        if cnf.enable.mem_watch {
            ALLOCMEASURE.reset();
            slf.watch_memory();
        }

        let docs = slf.internal.prefix(format!("{}", &DBLIST_PREFIX));
        for (_, db_name) in docs {
            slf.dbs.insert(db_name.to_owned(), Query::new_with_name(db_name.as_str()).await);
        }

        Ok(slf)
    }

    /// watch current memory usage
    fn watch_memory(&self) {
        std::thread::spawn(move || {
            loop {
                trace!("memory used: {}", Size::from_bytes(ALLOCMEASURE.get()).format().with_base(Base::Base10));
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        });
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

        let db = self.dbs.get(trim_apos(&user.db).as_str());
        if db.is_none() && user.db.ne(MAGIC_DB) {
            return QueryResult {
                data: vec![],
                error: FlinchError::SchemaError(DbError::DbNotExists(user.clone().db)),
                time_taken: ttk.done(),
            };
        }

        let parsed = parsed.unwrap();
        let permitted = match parsed {
            Flql::DbNew(_) => user.db.eq(MAGIC_DB),
            Flql::DbPerm(_) => self.auth.chk_permission(session_id, PermissionTypes::AssignUser),
            Flql::DbDrop(_) => user.db.eq(MAGIC_DB),
            Flql::New(_) => self.auth.chk_permission(session_id, PermissionTypes::CreateCollection),
            Flql::Drop(_) => self.auth.chk_permission(session_id, PermissionTypes::DropCollection),
            Flql::DropUser(_, _) => self.auth.chk_permission(session_id, PermissionTypes::AssignUser),
            Flql::Exists(_, _) => self.auth.chk_permission(session_id, PermissionTypes::Read),
            Flql::Length(_) => self.auth.chk_permission(session_id, PermissionTypes::Read),
            Flql::Flush(_) => self.auth.chk_permission(session_id, PermissionTypes::Flush),
            Flql::Ttl(_, _, _) => self.auth.chk_permission(session_id, PermissionTypes::Write),
            Flql::Put(_, _) => self.auth.chk_permission(session_id, PermissionTypes::Write),
            Flql::PutWhen(_, _, _) => self.auth.chk_permission(session_id, PermissionTypes::Write),
            Flql::PutPointer(_, _, _) => self.auth.chk_permission(session_id, PermissionTypes::Write),
            Flql::SearchTyping(_, _) => self.auth.chk_permission(session_id, PermissionTypes::Read),
            Flql::Get(_, _, _) => self.auth.chk_permission(session_id, PermissionTypes::Read),
            Flql::GetWhen(_, _, _, _) => self.auth.chk_permission(session_id, PermissionTypes::Read),
            Flql::GetPointer(_, _) => self.auth.chk_permission(session_id, PermissionTypes::Read),
            Flql::GetView(_, _) => self.auth.chk_permission(session_id, PermissionTypes::Read),
            Flql::GetClip(_, _) => self.auth.chk_permission(session_id, PermissionTypes::Read),
            Flql::GetIndex(_, _) => self.auth.chk_permission(session_id, PermissionTypes::Read),
            Flql::GetRange(_, _, _, _) => self.auth.chk_permission(session_id, PermissionTypes::Read),
            Flql::Delete(_) => self.auth.chk_permission(session_id, PermissionTypes::Write),
            Flql::DeleteWhen(_, _) => self.auth.chk_permission(session_id, PermissionTypes::Write),
            Flql::DeletePointer(_, _) => self.auth.chk_permission(session_id, PermissionTypes::Write),
            Flql::DeleteClip(_, _) => self.auth.chk_permission(session_id, PermissionTypes::Write),
            Flql::None => false
        };
        if !permitted {
            return QueryResult {
                data: vec![],
                error: FlinchError::SchemaError(DbError::UserNoPermission),
                time_taken: ttk.done(),
            };
        }
        match parsed {
            Flql::DbNew(permit) => {
                trace!("creating new database {:?}",&permit);
                let permit = self.convert_permit(permit.as_str());
                if permit.is_err() {
                    return QueryResult {
                        data: vec![],
                        error: FlinchError::SchemaError(permit.err().unwrap()),
                        time_taken: ttk.done(),
                    };
                }
                let permit = permit.unwrap();

                trace!("checking for creating new database");

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
                let res = self.drop(trim_apos(&db).as_str()).await;
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
            Flql::DropUser(db, user) => {
                let res = self.auth.drop_user(trim_apos(&db).as_str(), trim_apos(&user).as_str());
                QueryResult {
                    data: vec![],
                    error: match res {
                        Ok(_) => FlinchError::None,
                        Err(_) => FlinchError::SchemaError(res.err().unwrap()),
                    },
                    time_taken: ttk.done(),
                }
            }
            _ => {
                let db = db.unwrap();
                let db = db.value();
                db.exec_with_flql(parsed).await
            }
        }
    }

    async fn new(&self, permit: DbUser) -> anyhow::Result<(), DbError> {
        if self.dbs.contains_key(permit.db.as_str()) {
            trace!("cannot create db. {:?} exists", permit.db.as_str());
            return Err(DbError::DbExists(permit.db.clone()));
        }
        let db_name = permit.db.to_owned();
        trace!("init Query for db {:?}",db_name.as_str());
        let query = Query::new_with_name(db_name.as_str()).await;

        self.dbs.insert(db_name.to_owned(), query);
        self.internal.put_any(uuid(), format!("{}{}", &DBLIST_PREFIX, db_name));
        trace!("adding user {:?} for db", permit.name.as_str());

        self._new_user(permit)
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
        self._new_user(permit)
    }

    fn _new_user(&self, permit: DbUser) -> anyhow::Result<(), DbError> {
        let res = self.auth.add_user_with_hash(permit.clone());
        self.internal.put_any(format!("{}{}", &DBUSER_PREFIX, uuid()), permit);
        res
    }

    fn convert_permit(&self, permit: &str) -> anyhow::Result<DbUser, DbError> {
        let permit = serde_json::from_str::<DbUser>(permit);
        if permit.is_err() {
            return Err(DbError::InvalidPermissionConfig);
        }
        let permit = permit.unwrap();

        if !db_name_ok(permit.db.as_str()) {
            return Err(DbError::DbNameMalformed);
        }

        if permit.db.len() > MAX_DBNAME_LEN || permit.db.len() < MIN_DBNAME_LEN {
            return Err(DbError::InvalidNamingConventionLen(format!("Database"), MIN_DBNAME_LEN.to_string(), MAX_DBNAME_LEN.to_string()));
        }

        if permit.db.len() > MAX_USERNAME_LEN || permit.db.len() < MIN_USERNAME_LEN {
            return Err(DbError::InvalidNamingConventionLen(format!("Username"), MIN_USERNAME_LEN.to_string(), MAX_USERNAME_LEN.to_string()));
        }

        if permit.pw.len() < MIN_PW_LEN {
            return Err(DbError::MinPwLen(MIN_PW_LEN.to_string()));
        }

        Ok(permit)
    }

    async fn drop(&self, name: &str) -> anyhow::Result<(), DbError> {
        trace!("dropping db {:?}",&name);
        if !self.dbs.contains_key(&name.to_string()) {
            trace!("db does not exists");
            return Err(DbError::DbNotExists(name.to_string()));
        }
        {
            let query = self.dbs.get(name).unwrap();
            let instance = query.value();
            let db = instance.underlying_db();
            let ls = db.ls();
            for col in ls {
                let res = db.drop(col.as_str()).await;
                if res.is_err() {
                    error!("{:?}",res.err().unwrap());
                }
            }
            let res = db.delete_disk_dir();
            if res.is_err() {
                error!("failed to drop storage {}",name);
            } else {
                trace!("dropped tree {}",name);
            }
        }
        self.dbs.remove(name);
        self.auth.drop_by_db(name);

        Ok(())
    }
}
