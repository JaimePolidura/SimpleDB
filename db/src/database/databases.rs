use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use shared::{SimpleDbError, SimpleDbOptions, StorageValueMergeResult};
use crate::database::database::Database;
use crate::table::record::Record;

pub struct Databases {
    databases: SkipMap<String, Arc<Database>>,
    options: Arc<SimpleDbOptions>
}

impl Databases {
    pub fn create(
        options: Arc<SimpleDbOptions>,
    ) -> Result<Databases, SimpleDbError> {
        let options = shared::start_simpledb_options_builder_from(&options)
            .storage_value_merger(|prev, new| Self::merge_storage_tables(prev, new))
            .build_arc();

        let mut databases = Self::load_databases(&options)?;

        Ok(Databases {
            databases: Self::index_databases_by_name(&mut databases),
            options
        })
    }

    pub fn get_databases(&self) -> Vec<Arc<Database>> {
        let mut databases = Vec::new();
        for entry in self.databases.iter() {
            databases.push(entry.value().clone());
        }

        databases
    }

    pub fn get_database(&self, name: &str) -> Option<Arc<Database>> {
        self.databases.get(name)
            .map(|entry| entry.value().clone())
    }

    pub fn get_database_or_err(&self, name: &str) -> Result<Arc<Database>, SimpleDbError> {
        self.databases.get(name)
            .map(|entry| entry.value().clone())
            .ok_or(SimpleDbError::DatabaseNotFound(name.clone().to_string()))
    }

    pub fn create_database(&self, database_name: &str) -> Result<Arc<Database>, SimpleDbError> {
        if self.databases.contains_key(database_name) {
            return Err(SimpleDbError::DatabaseAlreadyExists(database_name.to_string()));
        }

        let database_options = self.build_database_options(database_name);
        let database = Database::create(&database_options, database_name)?;

        self.databases.insert(database_name.to_string(), database.clone());
        Ok(database)
    }

    fn build_database_options(&self, database_name: &str) -> Arc<SimpleDbOptions> {
        let mut database_path = PathBuf::from(&self.options.base_path);
        database_path.push(database_name);
        shared::start_simpledb_options_builder_from(&self.options)
            .base_path(database_path.to_str().unwrap())
            .build_arc()
    }

    fn index_databases_by_name(databases: &mut Vec<Arc<Database>>) -> SkipMap<String, Arc<Database>> {
        let mut indexed = SkipMap::new();

        while let Some(database) = databases.pop() {
            indexed.insert(database.name().clone(), database);
        }

        indexed
    }

    fn load_databases(options: &Arc<SimpleDbOptions>) -> Result<Vec<Arc<Database>>, SimpleDbError> {
        let mut databases: Vec<Arc<Database>> = Vec::new();

        for file in fs::read_dir(Path::new(&options.base_path))
            .map_err(|e| SimpleDbError::CannotReadDatabases(e))? {

            if file.is_ok() {
                let file = file.unwrap();

                let database_name = file.file_name();
                let database_name = database_name.to_str().unwrap();
                let database_options = shared::start_simpledb_options_builder_from(options)
                    .base_path(file.path().to_str().unwrap())
                    .build_arc();

                databases.push(Database::load_database(&database_options, database_name)?);
            }
        }

        Ok(databases)
    }

    fn merge_storage_tables(prev: &Bytes, new: &Bytes) -> StorageValueMergeResult {
        let tombstone = Bytes::new();
        if prev.eq(&tombstone) || new.eq(&tombstone) {
            StorageValueMergeResult::DiscardPrevious
        } else {
            let mut prev = Record::deserialize(prev.to_vec());
            let new = Record::deserialize(new.to_vec());
            prev.merge(new);

            StorageValueMergeResult::Ok(Bytes::from(prev.serialize()))
        }
    }
}