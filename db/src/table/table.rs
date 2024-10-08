use crate::database::database::Database;
use crate::index::index_creation_task::IndexCreationTask;
use crate::index::secondary_index_iterator::SecondaryIndexIterator;
use crate::index::secondary_indexes::SecondaryIndexes;
use crate::selection::Selection;
use crate::table::record::Record;
use crate::table::row::Row;
use crate::table::table_descriptor::{ColumnDescriptor, TableDescriptor};
use crate::table::table_flags::KEYSPACE_TABLE_USER;
use crate::table::table_iterator::TableIterator;
use crate::value::{Type, Value};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use shared::SimpleDbError::{CannotWriteTableDescriptor, ColumnNameAlreadyDefined, ColumnNotFound, IndexAlreadyExists, InvalidType, OnlyOnePrimaryColumnAllowed, PrimaryColumnNotIncluded, UnknownColumn};
use shared::{ColumnId, FlagMethods, KeyspaceId, SimpleDbError, SimpleDbFile, SimpleDbOptions};
use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{fence, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use storage::transactions::transaction::Transaction;
use storage::{SimpleDbStorageIterator, Storage};
use crate::index::index_type::IndexType;

pub struct Table {
    pub(crate) storage_keyspace_id: KeyspaceId,
    pub(crate) table_name: String,

    pub(crate) table_descriptor_file: Mutex<SimpleDbFile>,

    pub(crate) columns_by_id: SkipMap<ColumnId, ColumnDescriptor>,
    pub(crate) columns_by_name: SkipMap<String, ColumnId>,
    pub(crate) next_column_id: AtomicUsize,
    pub(crate) primary_column_name: String,

    pub(crate) storage: Arc<storage::Storage>,

    pub(crate) secondary_indexes: SecondaryIndexes,

    pub(crate) database: Arc<Database>
}

impl Table {
    pub(crate) fn create(
        table_name: &str,
        options: &Arc<shared::SimpleDbOptions>,
        storage: &Arc<storage::Storage>,
        primary_column_name: String,
        database: Arc<Database>
    ) -> Result<Arc<Table>, SimpleDbError> {
        let table_keyspace_id = storage.create_keyspace(KEYSPACE_TABLE_USER)?;
        let (table_descriptor, table_descriptor_file) = TableDescriptor::create(
            table_keyspace_id,
            options,
            table_name,
        )?;

        let max_column_id = table_descriptor.get_max_column_id();

        Ok(Arc::new(Table {
            table_descriptor_file: Mutex::new(table_descriptor_file),
            next_column_id: AtomicUsize::new(max_column_id as usize + 1),
            secondary_indexes: SecondaryIndexes::create_empty(storage.clone()),
            columns_by_id: table_descriptor.columns,
            table_name: table_descriptor.table_name,
            storage_keyspace_id: table_keyspace_id,
            columns_by_name: SkipMap::new(),
            storage: storage.clone(),
            primary_column_name,
            database
        }))
    }

    pub(crate) fn load_tables(
        options: &Arc<shared::SimpleDbOptions>,
        storage: &Arc<storage::Storage>,
        database: Arc<Database>
    ) -> Result<Vec<Arc<Table>>, SimpleDbError> {
        let mut tables = Vec::new();

        for keyspace_id in storage.get_keyspaces_id() {
            let flags = storage.get_flags(keyspace_id)?;

            if flags.has(KEYSPACE_TABLE_USER) {
                let (descriptor, descriptor_file) = TableDescriptor::load_from_disk(options, keyspace_id)?;
                tables.push(Arc::new(Table {
                    secondary_indexes: SecondaryIndexes::load_secondary_indexes(&descriptor, storage.clone()),
                    next_column_id: AtomicUsize::new(descriptor.get_max_column_id() as usize + 1),
                    columns_by_name: Self::index_column_id_by_name(&descriptor.columns),
                    primary_column_name: descriptor.get_primary_column_name(),
                    table_descriptor_file: Mutex::new(descriptor_file),
                    table_name: descriptor.table_name,
                    storage_keyspace_id: keyspace_id,
                    columns_by_id: descriptor.columns,
                    storage: storage.clone(),
                    database: database.clone()
                }));
            }
        }

        Ok(tables)
    }

    pub(crate) fn create_mock(columns: Vec<ColumnDescriptor>) -> Arc<Table> {
        let options = Arc::new(SimpleDbOptions::default());
        let mut columns_by_id = SkipMap::new();
        let mut primary_column_name = String::from("");
        for column in columns {
            if column.is_primary {
                primary_column_name = column.column_name.clone();
            }

            columns_by_id.insert(column.column_id, column);
        }

        Arc::new(Table {
            secondary_indexes: SecondaryIndexes::create_mock(options.clone()),
            columns_by_name: Self::index_column_id_by_name(&columns_by_id),
            table_descriptor_file: Mutex::new(SimpleDbFile::mock()),
            storage: Arc::new(Storage::create_mock(&options)),
            database: Database::create_mock(&options),
            next_column_id: AtomicUsize::new(1),
            table_name: String::from("Mock"),
            storage_keyspace_id: 1,
            primary_column_name,
            columns_by_id,
        })
    }

    pub fn add_columns(
        &self,
        columns_to_add: Vec<(String, Type, bool)>,
    ) -> Result<(), SimpleDbError> {
        for (column_name, column_type, is_primary) in columns_to_add {
            self.add_column(&column_name, column_type, is_primary)?
        }
        Ok(())
    }

    pub fn get_by_primary_column(
        self: &Arc<Self>,
        key: &Bytes,
        transaction: &Transaction,
        selection: &Selection
    ) -> Result<Option<Row>, SimpleDbError> {
        if selection.is_empty() {
            return Ok(None);
        }

        let mut table_iterator = TableIterator::create(
            self.storage.scan_from_key_with_transaction(
                transaction,
                self.storage_keyspace_id,
                key,
                true
            )?,
            self.selection_to_columns_id(selection)?,
            self.clone()
        );
        if !table_iterator.next() {
            return Ok(None);
        }

        let row = table_iterator.row();
        if row.get_primary_column_value().eq(key) {
            Ok(Some(row.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn scan_from_key(
        self: Arc<Self>,
        key: &Bytes,
        inclusive: bool,
        transaction: &Transaction,
        selection: Selection,
    ) -> Result<TableIterator<SimpleDbStorageIterator>, SimpleDbError> {
        let selection = self.selection_to_columns_id(&selection)?;
        let storage_iterator = self.storage.scan_from_key_with_transaction(
            transaction,
            self.storage_keyspace_id,
            key,
            inclusive
        )?;

        Ok(TableIterator::create(
            storage_iterator,
            selection,
            self.clone()
        ))
    }

    pub fn scan_all(
        self: Arc<Self>,
        transaction: &Transaction,
        selection: Selection
    ) -> Result<TableIterator<SimpleDbStorageIterator>, SimpleDbError> {
        let selection = self.selection_to_columns_id(&selection)?;
        let storage_iterator = self.storage.scan_all_with_transaction(transaction, self.storage_keyspace_id)?;

        Ok(TableIterator::create(
            storage_iterator,
            selection,
            self.clone()
        ))
    }

    pub fn scan_from_key_secondary_index(
        self: &Arc<Self>,
        key: &Bytes,
        transaction: &Transaction,
        column_name: &str
    ) -> Result<SecondaryIndexIterator<SimpleDbStorageIterator>, SimpleDbError> {
        let column_id = self.get_column_desc_or_err(column_name)?
            .column_id;
        let mut iterator = self.secondary_indexes.scan_all(transaction, column_id)?;
        iterator.seek(key, true);
        Ok(iterator)
    }

    pub fn scan_all_secondary_index(
        self: &Arc<Self>,
        transaction: &Transaction,
        column_name: &str
    ) -> Result<SecondaryIndexIterator<SimpleDbStorageIterator>, SimpleDbError> {
        let column_id = self.get_column_desc_or_err(column_name)?
            .column_id;
        self.secondary_indexes.scan_all(transaction, column_id)
    }

    pub fn create_secondary_index(
        self: &Arc<Self>,
        column_name: &str,
        wait: bool
    ) -> Result<usize, SimpleDbError> {
        let column = self.get_column_desc(column_name).unwrap();

        if self.secondary_indexes.has(column.column_id) {
            return Err(IndexAlreadyExists(self.storage_keyspace_id, column_name.to_string()));
        }

        let secondary_index_keyspace_id = self.secondary_indexes.create_new_secondary_index(column.column_id)?;
        //Before we start reading all the SSTables and Memtables, make sure the new secondary index is visible for writers
        fence(Ordering::Release);

        let task = IndexCreationTask::create(
            column.column_id,
            secondary_index_keyspace_id,
            self.database.clone(),
            self.storage.clone(),
            self.clone(),
        );

        let join_handle = std::thread::spawn(move || task.start());
        let mut n_affected_rows = 0;

        if wait {
            n_affected_rows = join_handle.join().unwrap();
        }

        self.save_column_descriptor_as_indexed(column.column_id, secondary_index_keyspace_id)?;

        Ok(n_affected_rows)
    }

    //Expect call to validate_insert before calling this function
    pub fn insert(
        self: Arc<Self>,
        transaction: &Transaction,
        to_insert_data: &mut Vec<(String, Bytes)>
    ) -> Result<(), SimpleDbError> {
        let id_value = self.extract_primary_value(to_insert_data).unwrap();
        self.upsert(transaction, id_value, true, to_insert_data)
    }

    pub fn delete(
        &self,
        transaction: &Transaction,
        id: Bytes
    ) -> Result<(), SimpleDbError> {
        self.storage.delete_with_transaction(
            self.storage_keyspace_id,
            transaction,
            id
        )
    }

    pub fn update(
        self: &Arc<Self>,
        transaction: &Transaction,
        id: Bytes,
        to_update_data: &Vec<(String, Bytes)>
    ) -> Result<(), SimpleDbError> {
        self.upsert(transaction, id, true, to_update_data)
    }

    fn upsert(
        self: &Arc<Self>,
        transaction: &Transaction,
        id: Bytes,
        is_new_values: bool,
        to_update_data: &Vec<(String, Bytes)>
    ) -> Result<(), SimpleDbError> {
        let new_record = self.build_record(to_update_data)?;
        let new_value = new_record.serialize();

        let old_record = Record::create(if !is_new_values {
            self.get_old_data_to_invalidate_secondary_index(&id, transaction, to_update_data)?
        } else {
            Vec::new()
        });

        self.storage.set_with_transaction(
            self.storage_keyspace_id,
            transaction,
            id.clone(),
            new_value.as_slice()
        )?;

        self.secondary_indexes.update_all(
            transaction,
            id,
            &new_record,
            &old_record
        )?;

        Ok(())
    }

    pub fn get_column_desc(
        &self,
        column_name: &str
    ) -> Option<ColumnDescriptor> {
        match self.columns_by_name.get(column_name) {
            Some(column_id) => {
                let column_data = self.columns_by_id.get(column_id.value()).unwrap();
                Some(column_data.value().clone())
            },
            None => None
        }
    }

    pub fn get_columns(&self) -> HashMap<String, ColumnDescriptor> {
        let mut columns = HashMap::new();
        for entry in self.columns_by_id.iter() {
            columns.insert(entry.value().column_name.clone(), entry.value().clone());
        }

        columns
    }

    pub fn validate_new_columns(
        columns: &Vec<(String, Type, bool)>,
    ) -> Result<(), SimpleDbError> {
        let mut primary_already_added = false;
        let mut column_names_added = HashSet::new();

        for (new_column_name, _, is_primary) in columns {
            let is_primary = *is_primary;

            if primary_already_added && is_primary {
                return Err(OnlyOnePrimaryColumnAllowed());
            }

            if !primary_already_added && is_primary {
                primary_already_added = true;
            }

            //Some value already exists
            if !column_names_added.insert(new_column_name) {
                return Err(ColumnNameAlreadyDefined(new_column_name.to_string()));
            }
        }

        if !primary_already_added {
            return Err(PrimaryColumnNotIncluded());
        }

        Ok(())
    }

    pub fn get_indexed_columns(&self) -> Vec<(String, IndexType)> {
        let mut indexed_columns = Vec::new();
        indexed_columns.push((self.primary_column_name.clone(), IndexType::Primary));
        for entry in self.columns_by_id.iter() {
            let column_desc = entry.value();
            if !column_desc.is_primary && column_desc.is_secondary_indexed() {
                indexed_columns.push((column_desc.column_name.clone(), IndexType::Secondary));
            }
        }

        indexed_columns
    }

    pub fn validate_selection(
        &self,
        selection: &Selection
    ) -> Result<(), SimpleDbError> {
        match selection {
            Selection::All => Ok(()),
            Selection::Some(selection) => {
                for column_name in selection {
                    if !self.columns_by_name.contains_key(column_name) {
                        return Err(ColumnNotFound(self.storage_keyspace_id, column_name.clone()));
                    }
                }

                Ok(())
            }
        }
    }

    pub fn validate_create_index(
        &self,
        column_name: &str
    ) -> Result<(), SimpleDbError> {
        let column = self.get_column_desc_or_err(column_name)?;

        if self.secondary_indexes.has(column.column_id) || column.is_primary{
            return Err(IndexAlreadyExists(self.storage_keyspace_id, column_name.to_string()));
        }

        Ok(())
    }

    pub fn validate_column_values(
        &self,
        to_insert_data: &Vec<(String, Value)>
    ) -> Result<(), SimpleDbError> {
        if !self.has_primary_value(to_insert_data) {
            return Err(PrimaryColumnNotIncluded())
        }
        for (column_name, column_value) in to_insert_data {
            match self.columns_by_name.get(column_name) {
                Some(column) => {
                    let column = self.columns_by_id.get(column.value()).unwrap();
                    let column = column.value();

                    if !column.column_type.can_be_casted(&column_value.to_type()) {
                        return Err(InvalidType(column_name.clone()));
                    }
                },
                None => return Err(UnknownColumn(column_name.clone())),
            }
        }

        Ok(())
    }

    fn has_primary_value(&self, data: &Vec<(String, Value)>) -> bool {
        for (column_name, _) in data.iter() {
            if column_name.eq(&self.primary_column_name) {
                return true
            }
        }

        false
    }

    fn extract_primary_value(&self, data: &mut Vec<(String, Bytes)>) -> Option<Bytes> {
        for (index, column_entry) in data.iter().enumerate() {
            let (column_name, _) = column_entry;
            if column_name.eq(&self.primary_column_name) {
                let (_, column_value) = data.remove(index);
                return Some(column_value);
            }
        }
        None
    }

    fn build_record(&self, data_records: &Vec<(String, Bytes)>) -> Result<Record, SimpleDbError> {
        let mut data_records_to_return: Vec<(ColumnId, Bytes)> = Vec::new();

        for (column_name, column_value) in data_records.iter() {
            let column = self.get_column_desc_or_err(column_name)?;
            data_records_to_return.push((column.column_id, column_value.clone()));
        }

        Ok(Record { data_records: data_records_to_return, })
    }

    fn add_column(
        &self,
        column_name: &str,
        column_type: Type,
        is_primary: bool,
    ) -> Result<(), SimpleDbError> {
        let column_descriptor = ColumnDescriptor {
            column_id: self.next_column_id.fetch_add(1, Relaxed) as shared::ColumnId,
            column_name: column_name.to_string(),
            secondary_index_keyspace_id: None,
            column_type,
            is_primary,
        };

        let mut file = self.table_descriptor_file.lock().unwrap();
        file.write(&column_descriptor.serialize())
            .map_err(|e| SimpleDbError::CannotWriteTableDescriptor(self.storage_keyspace_id, e))?;

        self.columns_by_name.insert(column_descriptor.column_name.clone(), column_descriptor.column_id);
        self.columns_by_id.insert(column_descriptor.column_id, column_descriptor);

        Ok(())
    }

    pub fn get_primary_column_data(&self) -> Option<ColumnDescriptor> {
        match self.columns_by_name.get(&self.primary_column_name) {
            Some(id) => {
                let value = self.columns_by_id.get(id.value())
                    .unwrap();
                Some(value.value().clone())
            },
            None => None
        }
    }

    fn index_column_id_by_name(columns_by_id: &SkipMap<ColumnId, ColumnDescriptor>) -> SkipMap<String, ColumnId> {
        let result = SkipMap::new();
        for entry in columns_by_id.iter() {
            result.insert(entry.value().column_name.clone(), *entry.key());
        }

        result
    }

    fn has_primary_key(&self) -> bool {
        self.columns_by_id.iter()
            .find(|i| i.value().is_primary)
            .is_some()
    }

    fn selection_to_columns_id(&self, selection: &Selection) -> Result<Vec<ColumnId>, SimpleDbError> {
        match selection {
            Selection::Some(columns_names) => {
                let mut column_ids = Vec::new();

                for column_name in columns_names {
                    let column_id = self.get_column_id_by_name(column_name)?;
                    column_ids.push(column_id);
                }

                Ok(column_ids)
            },
            Selection::All => {
                Ok(self.columns_by_id.iter()
                    .map(|entry| entry.key().clone())
                    .collect())
            }
        }
    }

    fn get_column_id_by_name(&self, column_name: &String) -> Result<ColumnId, SimpleDbError> {
        match self.columns_by_name.get(column_name) {
            Some(column_id) => Ok(*column_id.value()),
            None => Err(SimpleDbError::ColumnNotFound(self.storage_keyspace_id, column_name.to_string()))
        }
    }

    pub fn name(&self) -> &String {
        &self.table_name
    }

    fn get_old_data_to_invalidate_secondary_index(
        self: &Arc<Self>,
        key: &Bytes,
        transaction: &Transaction,
        updated_data: &Vec<(String, Bytes)>
    ) -> Result<Vec<(ColumnId, Bytes)>, SimpleDbError> {
        let mut old_data = Vec::new();

        let secondary_indexed_columns_names: Vec<String> = updated_data.iter()
            .map(|(column_name, _) | self.get_column_desc(column_name).unwrap())
            .filter(|column| column.is_secondary_indexed())
            .map(|column| column.column_name.clone())
            .collect();

        let old_value_selection = Selection::Some(secondary_indexed_columns_names);

        if !old_value_selection.is_empty() {
            if let Some(old_row_value) = self.get_by_primary_column(
                key,
                transaction,
                &old_value_selection,
            )? {
                for column_secondary_indexed_column_name in old_value_selection.get_some_selected_columns() {
                    let column_id = self.get_column_desc(&column_secondary_indexed_column_name)
                        .unwrap()
                        .column_id;

                    match old_row_value.get_column_value(&column_secondary_indexed_column_name)? {
                        Value::Null => continue,
                        value => old_data.push((column_id, value.serialize())),
                    };
                }
            }
        }

        Ok(old_data)
    }

    fn save_column_descriptor_as_indexed(
        &self,
        column_id_indexed: ColumnId,
        keyspace_id: KeyspaceId
    ) -> Result<(), SimpleDbError> {
        let mut file_lock = self.table_descriptor_file.lock().unwrap();

        //Create new list of columns descriptors
        let mut new_columns = Vec::new();
        for current_entry in self.columns_by_id.iter() {
            let current_column_id = *current_entry.key();
            let current_column = current_entry.value().clone();
            if current_column_id == column_id_indexed {
                let mut current_column_to_update = current_column;
                current_column_to_update.secondary_index_keyspace_id = Some(keyspace_id);
                new_columns.push(current_column_to_update.clone());
                self.columns_by_id.insert(current_column_id, current_column_to_update);
            } else {
                new_columns.push(current_column);
            }
        }

        //Save new table desc with updated column
        let serialized = TableDescriptor::serialize(new_columns, &self.primary_column_name);
        file_lock.safe_replace(&serialized)
            .map_err(|io_error| CannotWriteTableDescriptor(self.storage_keyspace_id, io_error))?;

        Ok(())
    }

    fn get_column_desc_or_err(
        &self,
        column_name: &str
    ) -> Result<ColumnDescriptor, SimpleDbError> {
        match self.get_column_desc(column_name) {
            Some(desc) => Ok(desc),
            None => Err(ColumnNotFound(self.storage_keyspace_id, column_name.to_string()))
        }
    }
}