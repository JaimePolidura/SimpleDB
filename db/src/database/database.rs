use crate::database::database_descriptor::DatabaseDescriptor;
use crate::table::table::Table;
use crossbeam_skiplist::SkipMap;
use shared::SimpleDbError::{CannotCreateDatabaseFolder, PrimaryColumnNotIncluded, TableAlreadyExists};
use shared::{utils, SimpleDbError, SimpleDbOptions};
use std::sync::{Arc, Mutex};
use storage::transactions::transaction::Transaction;
use crate::sql::statement::CreateTableStatement;
use crate::value::Type;

pub struct Database {
    name: String,
    storage: Arc<storage::Storage>,
    tables: SkipMap<String, Arc<Table>>,
    database_descriptor: Mutex<DatabaseDescriptor>,

    options: Arc<SimpleDbOptions>
}

impl Database {
    pub(crate) fn create(
        options: &Arc<SimpleDbOptions>,
        database_name: &str
    ) -> Result<Arc<Database>, SimpleDbError> {
        //Create database base folder
        utils::create_paths(&options.base_path)
            .map_err(|e| CannotCreateDatabaseFolder(database_name.to_string(), e))?;

        Ok(Arc::new(Database {
            database_descriptor: Mutex::new(DatabaseDescriptor::create(options, &database_name.to_string())?),
            storage: Arc::new(storage::create(options.clone())?),
            name: database_name.to_string(),
            options: options.clone(),
            tables: SkipMap::new(),
        }))
    }

    pub(crate) fn load_database(
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

    pub fn validate_create_table(
        &self,
        statement: &CreateTableStatement
    ) -> Result<(), SimpleDbError> {
        if self.tables.contains_key(&statement.table_name) {
            return Err(TableAlreadyExists(statement.table_name.to_string()))
        }

        Table::validate_new_columns(&statement.columns)?;
        Ok(())
    }

    pub fn create_table(
        &self,
        table_name: &str,
        columns: Vec<(String, Type, bool)>,
    ) -> Result<Arc<Table>, SimpleDbError> {
        let primary_column_name = columns.iter()
            .filter(|(_, _, is_primary)| *is_primary)
            .map(|(name,_, _)| name.clone())
            .find(|_| true) //Find first
            .ok_or(PrimaryColumnNotIncluded())?;

        let table = Table::create(
            table_name,
            &self.options,
            &self.storage,
            primary_column_name,
        )?;

        let mut lock_result = self.database_descriptor.lock();
        let database_descriptor = lock_result.as_mut().unwrap();
        database_descriptor.add_table(table_name, table.storage_keyspace_id)?;

        table.add_columns(columns)?;

        self.tables.insert(table.table_name.clone(), table.clone());

        Ok(table)
    }

    pub fn add_column(
        &self,
        table_name: &str,
        columns_to_add: Vec<(String, Type, bool)>
    ) -> Result<(), SimpleDbError> {
        let table = self.get_table_or_err(table_name)?;
        table.add_columns(columns_to_add)
    }

    pub fn get_table_or_err(&self, table_name: &str) -> Result<Arc<Table>, SimpleDbError> {
        self.tables.get(table_name)
            .map(|entry| entry.value().clone())
            .ok_or(SimpleDbError::TableNotFound(table_name.to_string()))
    }

    pub fn start_transaction(&self) -> Transaction {
        self.storage.start_transaction()
    }

    pub fn rollback_transaction(&self, transaction: &Transaction) -> Result<(), SimpleDbError> {
        self.storage.rollback_transaction(transaction)
    }

    pub fn commit_transaction(&self, transaction: &Transaction) -> Result<(), SimpleDbError> {
        self.storage.commit_transaction(transaction)
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn get_tables(&self) -> Vec<Arc<Table>> {
        let mut tables = Vec::new();
        for entry in self.tables.iter() {
            tables.push(entry.value().clone());
        }

        tables
    }

    fn index_by_table_name(tables: &mut Vec<Arc<Table>>) -> SkipMap<String, Arc<Table>> {
        let mut indexed = SkipMap::new();

        while let Some(table) = tables.pop() {
            indexed.insert(table.name().clone(), table);
        }

        indexed
    }

    fn validate_table_name(&self, table_name: &str) -> Result<(), SimpleDbError> {
        if self.tables.contains_key(table_name) {
            return Err(TableAlreadyExists(table_name.to_string()));
        }

        Ok(())
    }
}
