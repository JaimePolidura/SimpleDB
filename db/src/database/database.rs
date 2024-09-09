use crate::database::database_descriptor::DatabaseDescriptor;
use crate::table::table::Table;
use crossbeam_skiplist::SkipMap;
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::{Arc, Mutex};

pub struct Database {
    name: String,
    storage: Arc<storage::Storage>,
    tables: SkipMap<String, Arc<Table>>,
    database_descriptor: Mutex<DatabaseDescriptor>,

    options: Arc<shared::SimpleDbOptions>
}

impl Database {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn load_database(
        database_options: &Arc<SimpleDbOptions>,
        database_name: &str
    ) -> Result<Arc<Database>, SimpleDbError> {
        let storage = Arc::new(storage::create(database_options.clone())?);
        let tables = Table::load_tables(database_options, &storage)?;
        let database_descriptor = DatabaseDescriptor::load_database_descriptor(
            database_options,
            &String::from(database_name),
        )?;

        Ok(Arc::new(Database {
            database_descriptor: Mutex::new(database_descriptor),
            name: String::from(database_name),
            options: database_options.clone(),
            tables: SkipMap::new(),
            storage,
        }))
    }
}
