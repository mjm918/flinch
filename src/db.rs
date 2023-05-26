use anyhow::{Result};
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use log::{info, warn};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use sled::Db;
use crate::bkp::Bkp;
use crate::doc::{Document, ViewConfig};
use crate::err::{CollectionError};
use crate::col::{Collection};
use crate::utils::{COL_PREFIX, prefix_col_name};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CollectionOptions {
    pub name: String,
    pub index_opts: Vec<String>,
    pub search_opts: Vec<String>,
    pub view_opts: Vec<ViewConfig>,
    pub range_opts: Vec<String>,
    pub clips_opts: Vec<String>,
}

pub struct Database<D> where D: Document + 'static {
    storage: DashMap<String, Collection<D>>,
    persist: Db,
    internal_tree: Bkp
}

impl<D> Database<D>
     where D: Serialize + DeserializeOwned + Clone + Send + 'static + Document + Sync
{
    pub async fn init() -> Self {
        SimpleLogger::new()
            .with_colors(true)
            .with_level(log::LevelFilter::Debug)
            .init()
            .unwrap();

        let storage = DashMap::new();
        let persist = sled::open("./dbs").unwrap();
        let internal_tree = Bkp::new(&persist,"__flinch_internal");

        let existing = internal_tree.prefix(format!("{}demo",COL_PREFIX));
        for exi in existing {
            let options = serde_json::from_str::<CollectionOptions>(exi.1.as_str());
            if options.is_ok() {
                let options = options.unwrap();
                let name = &options.name;
                let col = Collection::<D>::new(&persist,options.to_owned());

                col.load_bkp().await;
                storage.insert(name.to_owned(), col);

                info!("boot collection {}",exi.0);
            } else {
                warn!("{} failed to load from local storage",exi.0);
            }
        }

        Self {
            storage,
            persist,
            internal_tree
        }
    }

    pub fn ls(&self) -> Vec<String> {
        self.storage.iter().map(|kv|kv.key().to_string()).collect::<Vec<String>>()
    }

    pub async fn add(&self, opts: CollectionOptions) -> Result<(), CollectionError> {
        let name = &opts.name.to_owned();
        if let Err(err) = self.exi(name.as_str()) {
            return Err(err);
        }
        self.storage.insert(name.to_owned(), Collection::<D>::new(&self.persist, opts.to_owned()));
        self.internal_tree.put_any(prefix_col_name(name.as_str()), opts).await;
        info!("collection - {} added",name);
        Ok(())
    }

    pub fn using(&self, name: &str) -> Result<Ref<String,Collection<D>>, CollectionError> {
        if let Some(col) = self.storage.get(name) {
            return Ok(col);
        }
        Err(CollectionError::NoSuchCollection)
    }

    pub async fn drop(&self, name: &str) -> Result<(), CollectionError> {
        if let Err(err) = self.exi(name) {
            return Err(err);
        }
        let col = self.using(name);
        let col = col.unwrap();
        col.value().empty().await;
        self.storage.remove(name);
        self.internal_tree.remove(prefix_col_name(name)).expect("remove from local storage");
        warn!("collection - {} dropped",name);
        Ok(())
    }

    fn exi(&self, name: &str) -> Result<(), CollectionError> {
        if let Some(_) = self.storage.get(name) {
            return Err(CollectionError::DuplicateCollection);
        }
        Ok(())
    }
}
