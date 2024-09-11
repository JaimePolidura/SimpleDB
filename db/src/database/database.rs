use crate::database::database_descriptor::DatabaseDescriptor;
use crate::table::table::Table;
use crossbeam_skiplist::SkipMap;
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::{Arc, Mutex};
use storage::transactions::transaction::Transaction;
use crate::table::table_descriptor::ColumnType;

pub struct Database {
    name: String,
    storage: Arc<storage::Storage>,
    tables: SkipMap<String, Arc<Table>>,
    database_descriptor: Mutex<DatabaseDescriptor>,

    options: Arc<shared::SimpleDbOptions>
}

impl Database {
    pub fn create_table(
        &self,
        table_name: &str,
        columns: Vec<(String, ColumnType, bool)>
    ) -> Result<Arc<Table>, SimpleDbError> {
        let table = Table::create(
            table_name,
            &self.options,
            &self.storage,
        )?;

        for (column_name, column_type, is_primary) in columns {
            table.add_column(
                column_name.as_str(),
                column_type,
                is_primary
            )?;
        }

        Ok(table)
    }

    pub fn get_table(&self, table_name: &str) -> Option<Arc<Table>> {
        self.tables.get(table_name)
            .map(|entry| entry.value().clone())
    }

    pub fn start_transaction(&self) -> Transaction {
        self.storage.start_transaction()
    }

    pub fn rollback_transaction(&self, transaction: Transaction) {
        self.storage.rollback_transaction(transaction)
    }

    pub fn commit_transaction(&self, transaction: Transaction) {
        self.storage.commit_transaction(transaction)
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn create(
        options: &Arc<SimpleDbOptions>,
        database_name: &str
    ) -> Result<Arc<Database>, SimpleDbError>{
        Ok(Arc::new(Database {
            database_descriptor: Mutex::new(DatabaseDescriptor::create(options, &database_name.to_string())?),
            storage: Arc::new(storage::create(options.clone())?),
            name: database_name.to_string(),
            options: options.clone(),
            tables: SkipMap::new(),
        }))
    }

    pub fn load_database(
        database_options: &Arc<SimpleDbOptions>,
        database_name: &str
    ) -> Result<Arc<Database>, SimpleDbError> {
        let storage = Arc::new(storage::create(database_options.clone())?);
        let mut tables = Table::load_tables(database_options, &storage)?;
        let database_descriptor = DatabaseDescriptor::load_database_descriptor(
            database_options,
            &String::from(database_name),
        )?;

        Ok(Arc::new(Database {
            database_descriptor: Mutex::new(database_descriptor),
            name: String::from(database_name),
            options: database_options.clone(),
            tables: Self::index_by_table_name(&mut tables),
            storage,
        }))
    }

    fn index_by_table_name(tables: &mut Vec<Arc<Table>>) -> SkipMap<String, Arc<Table>> {
        let mut indexed = SkipMap::new();

        while let Some(table) = tables.pop() {
            indexed.insert(table.name(), table);
        }

        indexed
    }
}
