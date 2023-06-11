use dashmap::DashMap;
use log::warn;
use sha256::digest as sha_hash;
use sled::Db;

use crate::errors::DbError;
use crate::headers::{DbName, DbUser, FlinchCnf, PermissionTypes, SessionId, UserName};
use crate::persistent::Persistent;
use crate::utils::{DBUSER_PREFIX, uuid};

pub struct Authenticate {
    storage: Persistent,
    users: DashMap<DbName, DashMap<UserName, DbUser>>,
    session: DashMap<SessionId, DbUser>,
}

impl Authenticate {
    pub fn new(db: &Db, cfg: &FlinchCnf) -> Self {
        let slf = Self {
            storage: Persistent::open(&db, "sys"),
            users: Default::default(),
            session: Default::default(),
        };
        let users = slf.storage.prefix(format!("{}", &DBUSER_PREFIX));
        for (_, user) in users {
            let json = serde_json::from_str::<DbUser>(user.as_str());
            if json.is_err() {
                warn!("user info from persistent storage failed to parse {}", user);
                continue;
            }
            let info = json.unwrap();
            slf.add_user(info);
        }
        let config = cfg.clone();
        let root = DbUser {
            name: config.root,
            pw: config.pw,
            db: "*".to_string(),
            create: true,
            drop: true,
            read: true,
            write: true,
            permit: true,
            flush: true,
        };
        slf.add_user_with_hash(root);
        slf
    }

    pub fn login(&self, username: &str, password: &str, db: &str) -> anyhow::Result<SessionId, DbError> {
        if let Some(users) = self.users.get(db) {
            let users = users.value();
            if let Some(user) = users.get(username) {
                let user = user.value();
                if user.pw.eq(sha_hash(password).as_str()) {
                    let session_id = uuid();
                    self.session.insert(session_id.clone(), user.clone());
                    Ok(session_id)
                } else {
                    Err(DbError::InvalidPassword)
                }
            } else {
                Err(DbError::NoSuchUser(username.to_string()))
            }
        } else {
            Err(DbError::DbNotExists(db.to_string()))
        }
    }

    pub fn add_user(&self, user: DbUser) {
        if let Some(mut col) = self.users.get_mut(user.db.as_str()) {
            let users = col.value_mut();
            let user_name = user.name.to_owned();
            users.insert(user_name, user);
        } else {
            let db = user.db.clone();
            let mut user_map = DashMap::new();
            user_map.insert(user.name.to_owned(), user);
            self.users.insert(db, user_map);
        }
    }

    pub fn add_user_with_hash(&self, user: DbUser) {
        let mut user = user;
        user.pw = sha_hash(user.pw);

        self.add_user(user.clone());
        self.storage.put_any(format!("{}{}", &DBUSER_PREFIX, uuid()), user);
    }

    pub fn drop_user(&self, db: &str, name: &str) -> anyhow::Result<(), DbError> {
        if let Some(mut col) = self.users.get_mut(db) {
            let user = col.value_mut();
            return if let Some(_) = user.get_mut(name) {
                user.remove(name);
                self.storage.remove(format!("{}{}", &DBUSER_PREFIX, name)).expect("removed user from persistent");
                Ok(())
            } else {
                Err(DbError::NoSuchUser(name.to_owned()))
            };
        }
        return Err(DbError::DbNotExists(db.to_string()));
    }

    pub fn chk_permission(&self, session_id: SessionId, perm_type: PermissionTypes) -> bool {
        if let Some(record) = self.session.get(session_id.as_str()) {
            let user = record.value();
            return match perm_type {
                PermissionTypes::AssignUser => user.permit,
                PermissionTypes::CreateCollection => user.create,
                PermissionTypes::DropCollection => user.drop,
                PermissionTypes::Read => user.read,
                PermissionTypes::Write => user.write,
                PermissionTypes::Flush => user.flush
            };
        }
        return false;
    }

    pub fn user(&self, session_id: SessionId) -> Option<DbUser> {
        if let Some(record) = self.session.get(session_id.as_str()) {
            let user = record.value();
            return Some(user.to_owned());
        }
        None
    }
}