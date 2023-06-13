use dashmap::DashMap;
use log::{trace, warn};
use sha256::digest as sha_hash;
use sled::Db;

use crate::errors::DbError;
use crate::headers::{DbName, DbUser, FlinchCnf, SessionId, UserName};
use crate::persistent::Persistent;
use crate::pri_headers::PermissionTypes;
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
        for (key, user) in users {
            trace!("{} user from persistent {:?}",key,&user);
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
            name: config.login.username,
            pw: config.login.password,
            db: "*".to_string(),
            create: true,
            drop: true,
            read: true,
            write: true,
            permit: true,
            flush: true,
        };
        let _ = slf.add_user_with_hash(root);
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
        trace!("adding user {:?} to memory",&user);
        if let Some(mut col) = self.users.get_mut(user.db.as_str()) {
            let users = col.value_mut();
            let user_name = user.name.to_owned();
            users.insert(user_name, user);
        } else {
            let db = user.db.clone();
            let user_map = DashMap::new();
            user_map.insert(user.name.to_owned(), user);
            self.users.insert(db, user_map);
        }
    }

    pub fn add_user_with_hash(&self, user: DbUser) -> anyhow::Result<(), DbError> {
        let mut user = user;
        user.pw = sha_hash(user.pw);

        for map in &self.users.get(user.db.as_str()) {
            let user_map = map.value();
            for u in user_map {
                let username = u.key();
                if username.eq(user.name.as_str()) {
                    return Err(DbError::UserExists(user.name.to_owned()));
                }
            }
        }

        self.add_user(user.clone());
        self.storage.put_any(format!("{}{}{}", &DBUSER_PREFIX, user.name.as_str(), uuid()), user);
        Ok(())
    }

    pub fn drop_user(&self, db: &str, name: &str) -> anyhow::Result<(), DbError> {
        trace!("removing user {:?} {:?}", db, name);
        if let Some(mut col) = self.users.get_mut(db) {
            let user = col.value_mut();
            if user.get_mut(name).is_none() {
                return Err(DbError::NoSuchUser(name.to_owned()));
            }
            user.remove(name);
            self.storage.remove_by_prefix(format!("{}{}", &DBUSER_PREFIX, name));

            return Ok(());
        }
        return Err(DbError::DbNotExists(db.to_string()));
    }

    pub fn chk_permission(&self, session_id: SessionId, perm_type: PermissionTypes) -> bool {
        trace!("permission check SessionID {:?} permission type {:?}",&session_id, &perm_type);
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

    pub fn drop_by_db(&self, db: &str) {
        trace!("removing database from auth");
        let _ = self.users.remove(format!("{}", &db).as_str());
        let mut session_id = format!("");
        for session in &self.session {
            let user = session.value();
            if user.db.eq(db) {
                session_id = format!("{}", session.key());
            }
        }
        self.session.remove(session_id.as_str());
    }
}