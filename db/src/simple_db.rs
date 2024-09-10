use std::fs;
use std::path::Path;
use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use shared::{SimpleDbError, SimpleDbOptions};
use crate::database::database::Database;

pub struct SimpleDb {
    databases: SkipMap<String, Arc<Database>>,
    options: Arc<shared::SimpleDbOptions>
}

impl SimpleDb {
    pub fn create(
        options: shared::SimpleDbOptions,
    ) -> Result<SimpleDb, SimpleDbError> {
        let mut databases = Self::load_databases(&options)?;

        Ok(SimpleDb{
            databases: Self::index_databases_by_name(&mut databases),
            options: Arc::new(options),
        })
    }

    pub fn get_database(&self, name: &str) -> Option<Arc<Database>> {
        self.databases.get(name)
            .map(|entry| entry.value().clone())
    }

    pub fn create_database(&self, name: &str) -> Result<Arc<Database>, SimpleDbError> {
        if self.databases.contains_key(name) {
            return Err(SimpleDbError::DatabaseAlreadyExists(name.to_string()));
        }

        let database = Database::create(&self.options, name)?;
        self.databases.insert(name.to_string(), database.clone());
        Ok(database)
    }

    fn index_databases_by_name(databases: &mut Vec<Arc<Database>>) -> SkipMap<String, Arc<Database>> {
        let mut indexed = SkipMap::new();

        while let Some(database) = databases.pop() {
            indexed.insert(database.name(), database);
        }

        indexed
    }

    fn load_databases(options: &SimpleDbOptions) -> Result<Vec<Arc<Database>>, SimpleDbError> {
        let mut databases: Vec<Arc<Database>> = Vec::new();

        for file in fs::read_dir(Path::new(&options.base_path))
            .map_err(|e| shared::SimpleDbError::CannotReadDatabases(e))? {

            if file.is_ok() {
                let file = file.unwrap();

                let database_name = file.file_name();
                let database_name = database_name.to_str().unwrap();
                let database_options = shared::start_simpledb_options_builder_from(options)
                    .base_path(file.path().to_str().unwrap())
                    .build();

                databases.push(Database::load_database(&database_options, database_name)?);
            }
        }

        Ok(databases)
    }
}