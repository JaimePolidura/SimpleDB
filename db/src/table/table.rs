use crate::database::database::Database;
use crate::index::index_creation_task::IndexCreationTask;
use crate::index::secondary_index_iterator::SecondaryIndexIterator;
use crate::index::secondary_indexes::SecondaryIndexes;
use crate::selection::Selection;
use crate::table::record::Record;
use crate::table::row::Row;
use crate::table::schema::{Column, Schema};
use crate::table::table_descriptor::TableDescriptor;
use crate::table::table_flags::KEYSPACE_TABLE_USER;
use crate::table::table_iterator::TableIterator;
use bytes::Bytes;
use shared::SimpleDbError::{ColumnNameAlreadyDefined, ColumnNotFound, IndexAlreadyExists, InvalidType, OnlyOnePrimaryColumnAllowed, PrimaryColumnNotIncluded, UnknownColumn};
use shared::{ColumnId, FlagMethods, KeyspaceId, SimpleDbError, SimpleDbOptions, Type, Value};
use std::collections::HashSet;
use std::sync::atomic::{fence, Ordering};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use storage::{SimpleDbStorageIterator, Storage};

pub struct Table {
    pub(crate) storage_keyspace_id: KeyspaceId,
    pub(crate) table_name: String,

    pub(crate) storage: Arc<storage::Storage>,

    pub(crate) secondary_indexes: SecondaryIndexes,

    pub(crate) database: Arc<Database>,

    pub(crate) table_descriptor: TableDescriptor
}

impl Table {
    pub(crate) fn create(
        table_name: &str,
        columns: Vec<(String, Type, bool)>,
        options: &Arc<shared::SimpleDbOptions>,
        storage: &Arc<storage::Storage>,
        database: Arc<Database>
    ) -> Result<Arc<Table>, SimpleDbError> {
        let primary_column_type = columns.iter()
            .find(|(_, _, is_primary)| *is_primary)
            .map(|(_, column_type, _)| column_type.clone())
            .ok_or(PrimaryColumnNotIncluded())?;

        let table_keyspace_id = storage.create_keyspace(
            KEYSPACE_TABLE_USER,
            primary_column_type
        )?;
        let table_descriptor = TableDescriptor::create(
            table_keyspace_id,
            options,
            table_name,
            columns
        )?;

        Ok(Arc::new(Table {
            secondary_indexes: SecondaryIndexes::create_empty(storage.clone(), table_name, primary_column_type),
            table_name: table_descriptor.table_name.clone(),
            storage_keyspace_id: table_keyspace_id,
            storage: storage.clone(),
            table_descriptor,
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
                let table_descriptor = TableDescriptor::load_from_disk(options, keyspace_id)?;
                tables.push(Arc::new(Table {
                    secondary_indexes: SecondaryIndexes::load_secondary_indexes(
                        storage.clone(),
                        table_descriptor.table_name.clone(),
                        table_descriptor.schema.clone()
                    ),
                    table_name: table_descriptor.table_name.clone(),
                    storage_keyspace_id: keyspace_id,
                    database: database.clone(),
                    storage: storage.clone(),
                    table_descriptor
                }));
            }
        }

        Ok(tables)
    }

    pub(crate) fn create_mock(columns: Vec<Column>) -> Arc<Table> {
        let options = Arc::new(SimpleDbOptions::default());
        Arc::new(Table {
            secondary_indexes: SecondaryIndexes::create_mock(options.clone()),
            table_descriptor: TableDescriptor::create_mock(columns),
            storage: Arc::new(Storage::create_mock(&options)),
            database: Database::create_mock(&options),
            table_name: String::from("Mock"),
            storage_keyspace_id: 1,
        })
    }

    pub fn add_columns(
        &self,
        columns_to_add: Vec<(String, Type, bool)>,
    ) -> Result<(), SimpleDbError> {
        for (column_name, column_type, is_primary) in columns_to_add {
            self.table_descriptor.add_column(&column_name, column_type, is_primary)?;
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
            selection.to_columns_id(self.get_schema())?,
            self.clone()
        );
        if !table_iterator.next() {
            return Ok(None);
        }

        let row = table_iterator.row();

        if row.get_primary_column_value().eq_bytes(key) {
            Ok(Some(row.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn scan_from_key(
        self: &Arc<Self>,
        key: &Bytes,
        inclusive: bool,
        transaction: &Transaction,
        selection: &Selection,
    ) -> Result<TableIterator<SimpleDbStorageIterator>, SimpleDbError> {
        let selection = selection.to_columns_id(self.get_schema())?;
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
        self: &Arc<Self>,
        transaction: &Transaction,
        selection: &Selection
    ) -> Result<TableIterator<SimpleDbStorageIterator>, SimpleDbError> {
        let selection = selection.to_columns_id(self.get_schema())?;
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
        inclusive: bool,
        transaction: &Transaction,
        column_name: &str,
    ) -> Result<SecondaryIndexIterator<SimpleDbStorageIterator>, SimpleDbError> {
        let schema = self.get_schema();
        let column_id = schema.get_column_or_err(column_name)?
            .column_id;

        let mut iterator = self.secondary_indexes.scan_all(transaction, column_id)?;
        iterator.seek(key, inclusive);
        Ok(iterator)
    }

    pub fn scan_all_secondary_index(
        self: &Arc<Self>,
        transaction: &Transaction,
        column_name: &str
    ) -> Result<SecondaryIndexIterator<SimpleDbStorageIterator>, SimpleDbError> {
        let schema = self.get_schema();
        let column_id = schema.get_column_or_err(column_name)?
            .column_id;
        self.secondary_indexes.scan_all(transaction, column_id)
    }

    pub fn create_secondary_index(
        self: &Arc<Self>,
        column_name_to_be_indexed: &str,
        wait: bool
    ) -> Result<usize, SimpleDbError> {
        let column_to_be_indexed = self.get_column(column_name_to_be_indexed).unwrap();

        if self.secondary_indexes.can_be_read(column_to_be_indexed.column_id) {
            return Err(IndexAlreadyExists(self.storage_keyspace_id, column_name_to_be_indexed.to_string()));
        }

        let index_keyspace_id = self.secondary_indexes.create_new_secondary_index(column_to_be_indexed.clone())?;
        //Before we start reading all the SSTables and Memtables, make sure the new secondary index is visible for writers
        fence(Ordering::Release);

        let (task, receiver) = IndexCreationTask::create(
            column_to_be_indexed.clone(),
            index_keyspace_id,
            self.storage_keyspace_id,
            self.database.clone(),
            self.storage.clone(),
            self.clone(),
        );

        let _ = std::thread::spawn(move || task.start());
        let mut n_affected_rows = 0;

        if wait {
            match receiver.recv().unwrap() {
                Ok(n) => n_affected_rows = n,
                Err(err) => {
                    //TODO Delete keyspace & secondary index
                    return Err(err);
                }
            }
        }

        self.table_descriptor.update_column_secondary_index(
            column_to_be_indexed.column_id,
            index_keyspace_id
        )?;

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

    pub fn get_column(
        &self,
        column_name: &str
    ) -> Option<Column> {
        let schema = self.table_descriptor.get_schema();
        schema.get_column(column_name)
    }

    pub fn get_schema(&self) -> &Schema {
        self.table_descriptor.get_schema()
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

    pub fn validate_selection(
        &self,
        selection: &Selection
    ) -> Result<(), SimpleDbError> {
        match selection {
            Selection::All => Ok(()),
            Selection::Some(selection) => {
                let schema = self.table_descriptor.get_schema();
                for column_name in selection {
                    if schema.get_column(column_name).is_none() {
                        return Err(ColumnNotFound(column_name.clone()));
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
        let schema = self.get_schema();
        let column = schema.get_column_or_err(column_name)?;

        if self.secondary_indexes.can_be_read(column.column_id) || column.is_primary{
            return Err(IndexAlreadyExists(self.storage_keyspace_id, column_name.to_string()));
        }

        Ok(())
    }

    pub fn validate_insert_column_values(
        &self,
        to_insert_data: &Vec<(String, Value)>
    ) -> Result<(), SimpleDbError> {
        let schema = self.table_descriptor.get_schema();

        if !self.validate_has_primary_value(to_insert_data) {
            return Err(PrimaryColumnNotIncluded())
        }
        for (column_name, column_value) in to_insert_data {
            match schema.get_column(column_name) {
                Some(column) => {
                    if !column.column_type.can_be_casted(&column_value.get_type()) {
                        return Err(InvalidType(column_name.clone()));
                    }
                },
                None => return Err(UnknownColumn(column_name.clone())),
            }
        }

        Ok(())
    }

    fn validate_has_primary_value(&self, data: &Vec<(String, Value)>) -> bool {
        let schema = self.table_descriptor.get_schema();
        let primary_column_name = schema.get_primary_column();
        let primary_column_name = primary_column_name.column_name.clone();

        for (column_name, _) in data.iter() {
            if column_name.eq(&primary_column_name) {
                return true
            }
        }

        false
    }

    fn extract_primary_value(&self, data: &mut Vec<(String, Bytes)>) -> Option<Bytes> {
        let schema = self.table_descriptor.get_schema();
        let primary_column_name = schema.get_primary_column();
        let primary_column_name = primary_column_name.column_name.clone();

        for (index, column_entry) in data.iter().enumerate() {
            let (column_name, _) = column_entry;
            if column_name.eq(&primary_column_name) {
                let (_, column_value) = data.remove(index);
                return Some(column_value);
            }
        }
        None
    }

    fn build_record(&self, data_records: &Vec<(String, Bytes)>) -> Result<Record, SimpleDbError> {
        let mut data_records_to_return: Vec<(ColumnId, Bytes)> = Vec::new();
        let schema = self.get_schema();

        for (column_name, column_value) in data_records.iter() {
            let column = schema.get_column_or_err(column_name)?;
            data_records_to_return.push((column.column_id, column_value.clone()));
        }

        Ok(Record { data_records: data_records_to_return, })
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
            .map(|(column_name, _) | self.get_column(column_name).unwrap())
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
                    let column_id = self.get_column(&column_secondary_indexed_column_name)
                        .unwrap()
                        .column_id;

                    match old_row_value.get_column_value(&column_secondary_indexed_column_name)? {
                        _ => continue,
                        value => old_data.push((column_id, value.get_bytes().clone())),
                    };
                }
            }
        }

        Ok(old_data)
    }
}