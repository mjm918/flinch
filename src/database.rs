use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use sled::Db;

use crate::collection::Collection;
use crate::doc_trait::{Document, ViewConfig};
use crate::errors::CollectionError;
use crate::persistent::Persistent;
use crate::pri_headers::INTERNAL_COL;
use crate::utils::{COL_PREFIX, database_path, get_col_name, prefix_col_name};

/// `CollectionOptions` is used while creating a collection
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CollectionOptions {
    pub name: String,
    pub index_opts: Vec<String>,
    pub search_opts: Vec<String>,
    pub view_opts: Vec<ViewConfig>,
    pub range_opts: Vec<String>,
    pub clips_opts: Vec<String>,
}

/// `Database<D>` keeps a bunch of collections. Where `D` inherits `Document`
pub struct Database<D> where D: Document + 'static {
    storage: Arc<DashMap<String, Arc<Collection<D>>>>,
    persist: Db,
    internal_tree: Persistent,
    db_path: String,
}

impl<D> Database<D>
    where D: Serialize + DeserializeOwned + Clone + Send + 'static + Document + Sync
{
    /// Creates an instance of Flinch database.
    /// Flinch uses sled as persistent storage
    /// Initialise:
    /// 1. logging
    /// 2. creates persistent storage
    /// 3. boots previously created collections
    pub async fn init() -> Self {
        Self::boot(None).await
    }

    pub async fn init_with_name(name: &str) -> Self {
        Self::boot(Some(name.to_string())).await
    }

    async fn boot(name: Option<String>) -> Self {
        let storage = DashMap::new();
        let db_path = database_path(name);
        let persist = sled::open(db_path.as_str()).unwrap();

        let internal_tree = Persistent::open(&persist, INTERNAL_COL);

        let existing = internal_tree.prefix(format!("{}", COL_PREFIX));
        for exi in existing {
            let options = serde_json::from_str::<CollectionOptions>(exi.1.as_str());
            if options.is_ok() {
                let options = options.unwrap();
                let name = &options.name;
                let col = Collection::<D>::new(&persist, options.to_owned()).await;

                storage.insert(name.to_owned(), col);
                info!("booted collection {}",get_col_name(exi.0.as_str()));
            } else {
                warn!("{} failed to load from local storage",exi.0);
            }
        }
        let instance = Self {
            storage: Arc::new(storage),
            persist,
            internal_tree,
            db_path,
        };
        instance
    }

    /// `ls` list out all the collections in the database
    pub fn ls(&self) -> Vec<String> {
        self.storage.iter().map(|kv| kv.key().to_string()).collect::<Vec<String>>()
    }

    /// `add` allows you to create collection. Required argument `CollectionOptions`
    pub async fn add(&self, opts: CollectionOptions) -> Result<(), CollectionError> {
        let name = &opts.name.to_owned();
        if let Err(err) = self.exi(name.as_str()) {
            return Err(err);
        }
        let col = Collection::<D>::new(&self.persist, opts.to_owned()).await;
        self.storage.insert(name.to_owned(), col);
        self.internal_tree.put_any(prefix_col_name(name.as_str()), opts);

        info!("collection - {} added",name);

        Ok(())
    }

    /// `using` returns a session of a collection by `name`
    pub fn using(&self, name: &str) -> Result<Ref<String, Arc<Collection<D>>>, CollectionError> {
        if let Some(col) = self.storage.get(name) {
            return Ok(col);
        }
        Err(CollectionError::NoSuchCollection)
    }

    /// `drop` drops a collection by `name`
    pub async fn drop(&self, name: &str) -> Result<(), CollectionError> {
        if let None = self.storage.get(name) {
            return Err(CollectionError::NoSuchCollection);
        }
        {
            let col = self.using(name);
            let col = col.unwrap();
            col.value().empty().await;
        }
        self.storage.remove(name);
        self.internal_tree.remove(prefix_col_name(name)).expect("remove from local storage");
        self.persist.drop_tree(name).expect("drop collection from local storage");

        warn!("collection - {} dropped",name);

        Ok(())
    }

    pub fn delete_disk_dir(&self) -> std::io::Result<()> {
        std::fs::remove_dir_all(self.db_path.as_str())
    }

    fn exi(&self, name: &str) -> Result<(), CollectionError> {
        if let Some(_) = self.storage.get(name) {
            return Err(CollectionError::DuplicateCollection);
        }
        Ok(())
    }
}
