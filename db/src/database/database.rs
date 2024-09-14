use crate::database::database_descriptor::DatabaseDescriptor;
use crate::table::table::Table;
use crate::table::table_descriptor::ColumnType;
use crossbeam_skiplist::SkipMap;
use shared::SimpleDbError::{ColumnNameAlreadyDefined, NotPrimaryColumnDefined, OnlyOnePrimaryColumnAllowed, TableAlreadyExists};
use shared::{SimpleDbError, SimpleDbOptions};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use storage::transactions::transaction::Transaction;

pub struct Database {
    name: String,
    storage: Arc<storage::Storage>,
    tables: SkipMap<String, Arc<Table>>,
    database_descriptor: Mutex<DatabaseDescriptor>,

    options: Arc<SimpleDbOptions>
}

impl Database {
    pub fn create_table(
        &self,
        table_name: &str,
        columns: Vec<(String, ColumnType, bool)>
    ) -> Result<Arc<Table>, SimpleDbError> {
        self.validate_table_creation(table_name)?;
        self.validate_new_table_columns(&columns, true)?;

        let table = Table::create(
            table_name,
            &self.options,
            &self.storage,
        )?;

        let mut lock_result = self.database_descriptor.lock();
        let database_descriptor = lock_result.as_mut().unwrap();
        database_descriptor.add_table(table_name, table.storage_keyspace_id)?;

        for (column_name, column_type, is_primary) in columns {
            table.add_column(
                column_name.as_str(),
                column_type,
                is_primary
            )?;
        }

        Ok(table)
    }

    pub fn add_column(
        &self,
        table_name: &str,
        columns_to_add: Vec<(String, ColumnType, bool)>
    ) -> Result<(), SimpleDbError> {
        self.validate_new_table_columns(&columns_to_add, false)?;
        let table = self.get_table(table_name)?;

        for (column_name, column_type, _) in columns_to_add {
            table.add_column(
                column_name.as_str(),
                column_type,
                false
            )?;
        }

        Ok(())
    }

    pub fn get_table(&self, table_name: &str) -> Result<Arc<Table>, SimpleDbError> {
        self.tables.get(table_name)
            .map(|entry| entry.value().clone())
            .ok_or(SimpleDbError::TableNotFound(table_name.to_string()))
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

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn create(
        options: &Arc<SimpleDbOptions>,
        database_name: &str
    ) -> Result<Arc<Database>, SimpleDbError> {
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

    fn validate_table_creation(&self, table_name: &str) -> Result<(), SimpleDbError> {
        if self.tables.contains_key(table_name) {
            return Err(TableAlreadyExists(table_name.to_string()));
        }

        Ok(())
    }

    fn validate_new_table_columns(
        &self,
        columns: &Vec<(String, ColumnType, bool)>,
        called_at_table_creation: bool,
    ) -> Result<(), SimpleDbError> {
        let mut primary_already_added = false;
        let mut column_names_added = HashSet::new();

        for (new_column_name, _, is_primary) in columns {
            let is_primary = *is_primary;

            if called_at_table_creation && is_primary {
                return Err(OnlyOnePrimaryColumnAllowed());
            }

            if primary_already_added && is_primary && called_at_table_creation {
                return Err(OnlyOnePrimaryColumnAllowed());
            } else if !primary_already_added && is_primary && called_at_table_creation{
                primary_already_added = true;
            }

            //Some value already exists
            if !column_names_added.insert(new_column_name) {
                return Err(ColumnNameAlreadyDefined(new_column_name.to_string()));
            }
        }

        if !primary_already_added && called_at_table_creation {
            return Err(NotPrimaryColumnDefined());
        }

        Ok(())
    }
}
