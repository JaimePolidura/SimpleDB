use crate::database::database_descriptor::DatabaseDescriptor;
use crate::sql::statement::CreateTableStatement;
use crate::table::table::Table;
use crossbeam_skiplist::SkipMap;
use shared::SimpleDbError::{CannotCreateDatabaseFolder, TableAlreadyExists};
use shared::{utils, SimpleDbError, SimpleDbOptions, Type};
use std::sync::{Arc, LockResult, Mutex, RwLock, RwLockWriteGuard};
use storage::transactions::transaction::Transaction;
use storage::Storage;

pub struct Database {
    name: String,
    storage: Arc<storage::Storage>,
    tables: SkipMap<String, Arc<Table>>,
    database_descriptor: Mutex<DatabaseDescriptor>,

    options: Arc<SimpleDbOptions>,

    //See self::lock_rollbacks() method docks
    rollback_lock: RwLock<()>,
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
            storage: Arc::new(Storage::create(options.clone())?),
            rollback_lock: RwLock::new(()),
            name: database_name.to_string(),
            options: options.clone(),
            tables: SkipMap::new(),
        }))
    }

    pub(crate) fn load_database(
        database_options: &Arc<SimpleDbOptions>,
        database_name: &str
    ) -> Result<Arc<Database>, SimpleDbError> {
        let storage = Arc::new(Storage::create(database_options.clone())?);
        let database_descriptor = DatabaseDescriptor::load_database_descriptor(
            database_options,
            &String::from(database_name),
        )?;

        let database = Arc::new(Database {
            database_descriptor: Mutex::new(database_descriptor),
            name: String::from(database_name),
            options: database_options.clone(),
            rollback_lock: RwLock::new(()),
            storage: storage.clone(),
            tables: SkipMap::new(),
        });

        database.set_tables(Table::load_tables(database_options, &storage, database.clone())?);

        Ok(database)
    }

    pub(crate) fn create_mock(options: &Arc<SimpleDbOptions>) -> Arc<Database> {
        Arc::new(Database {
            database_descriptor: Mutex::new(DatabaseDescriptor::mock()),
            storage: Arc::new(Storage::create_mock(&options.clone())),
            rollback_lock: RwLock::new(()),
            name: String::from("mock"),
            tables: SkipMap::new(),
            options: options.clone()
        })
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
        self: &Arc<Self>,
        table_name: &str,
        columns: Vec<(String, Type, bool)>,
    ) -> Result<Arc<Table>, SimpleDbError> {
        let table = Table::create(
            table_name,
            columns,
            &self.options,
            &self.storage,
            self.clone()
        )?;

        let mut lock_result = self.database_descriptor.lock();
        let database_descriptor = lock_result.as_mut().unwrap();
        database_descriptor.add_table(table_name, table.storage_keyspace_id)?;

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
        let lock = self.rollback_lock.read();
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

    fn validate_table_name(&self, table_name: &str) -> Result<(), SimpleDbError> {
        if self.tables.contains_key(table_name) {
            return Err(TableAlreadyExists(table_name.to_string()));
        }

        Ok(())
    }

    //Only used at creation time
    pub(crate) fn set_tables(&self, mut tables: Vec<Arc<Table>>) {
        while let Some(table) = tables.pop() {
            self.tables.insert(table.name().clone(), table);
        }
    }

    //This is only used to solve a race condition when a secondary index is being created
    //and a transaction is rolledback.
    pub(crate) fn lock_rollbacks(&self) -> LockResult<RwLockWriteGuard<'_, ()>> {
        self.rollback_lock.write()
    }
}
