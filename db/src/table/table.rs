use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::hash::Hasher;
use crate::table::table_descriptor::{ColumnDescriptor, ColumnType, TableDescriptor};
use shared::{ColumnId, SimpleDbError, SimpleDbFileWrapper};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use shared::SimpleDbError::{ColumnNameAlreadyDefined, NotPrimaryColumnDefined, OnlyOnePrimaryColumnAllowed};
use storage::SimpleDbStorageIterator;
use storage::transactions::transaction::Transaction;
use crate::selection::Selection;
use crate::table::record::Record;
use crate::table::table_iteartor::TableIterator;

pub struct Table {
    pub(crate) storage_keyspace_id: shared::KeyspaceId,
    pub(crate) table_name: String,

    pub(crate) table_descriptor_file: SimpleDbFileWrapper,

    pub(crate) columns_by_id: SkipMap<ColumnId, ColumnDescriptor>,
    pub(crate) columns_by_name: SkipMap<String, shared::ColumnId>,
    pub(crate) next_column_id: AtomicUsize,

    pub(crate) storage: Arc<storage::Storage>,
}

impl Table {
    pub fn add_columns(
        &self,
        columns_to_add: Vec<(String, ColumnType, bool)>,
    ) -> Result<(), SimpleDbError> {
        self.validate_new_table_columns(&columns_to_add)?;
        for (column_name, column_type, is_primary) in columns_to_add {
            self.add_column(&column_name, column_type, is_primary)?
        }
        Ok(())
    }

    pub fn scan_from_key(
        self: Arc<Self>,
        key: &Bytes,
        transaction: &Transaction,
        selection: Selection,
    ) -> Result<TableIterator, SimpleDbError> {
        let selection = self.selection_to_columns_id(selection)?;
        let storage_iterator = self.storage.scan_from_key_with_transaction(transaction, self.storage_keyspace_id, key)?;

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
    ) -> Result<TableIterator, SimpleDbError> {
        let selection = self.selection_to_columns_id(selection)?;
        let storage_iterator = self.storage.scan_all_with_transaction(transaction, self.storage_keyspace_id)?;

        Ok(TableIterator::create(
            storage_iterator,
            selection,
            self.clone()
        ))
    }

    pub fn insert(
        &self,
        transaction: &Transaction,
        id: Bytes,
        to_insert_data: &Vec<(String, Bytes)>
    ) -> Result<(), SimpleDbError> {
        self.update(transaction, id, to_insert_data)
    }

    pub fn update(
        &self,
        transaction: &Transaction,
        id: Bytes,
        to_update_data: &Vec<(String, Bytes)>
    ) -> Result<(), SimpleDbError> {
        let record = self.build_record(to_update_data)?;
        let value = record.serialize();

        self.storage.set_with_transaction(
            self.storage_keyspace_id,
            transaction,
            id,
            value.as_slice()
        )
    }

    fn build_record(&self, data_records: &Vec<(String, Bytes)>) -> Result<Record, SimpleDbError> {
        let mut data_records_to_return: Vec<(ColumnId, Bytes)> = Vec::new();

        for (column_name, column_value) in data_records.iter() {
            match self.columns_by_name.get(column_name) {
                Some(entry) => data_records_to_return.push((*entry.value(), column_value.clone())),
                None => return Err(SimpleDbError::ColumnNotFound(self.storage_keyspace_id, column_name.clone())),
            };
        }

        Ok(Record { data_records: data_records_to_return, })
    }

    fn add_column(
        &self,
        column_name: &str,
        column_type: ColumnType,
        is_primary: bool,
    ) -> Result<(), SimpleDbError> {
        let column_descriptor = ColumnDescriptor {
            column_id: self.next_column_id.fetch_add(1, Relaxed) as shared::ColumnId,
            column_name: column_name.to_string(),
            column_type,
            is_primary,
        };

        let file = unsafe { &mut *self.table_descriptor_file.file.get() };
        file.write(&column_descriptor.serialize())
            .map_err(|e| SimpleDbError::CannotWriteTableDescriptor(self.storage_keyspace_id, e))?;

        self.columns_by_name.insert(column_descriptor.column_name.clone(), column_descriptor.column_id);
        self.columns_by_id.insert(column_descriptor.column_id, column_descriptor);

        Ok(())
    }

    pub fn create(
        table_name: &str,
        options: &Arc<shared::SimpleDbOptions>,
        storage: &Arc<storage::Storage>,
    ) -> Result<Arc<Table>, SimpleDbError> {
        let table_keyspace_id = storage.create_keyspace()?;
        let (table_descriptor, table_descriptor_file) = TableDescriptor::create(
            table_keyspace_id,
            options,
            table_name
        )?;

        let max_column_id = table_descriptor.get_max_column_id();

        Ok(Arc::new(Table {
            storage_keyspace_id: table_keyspace_id,
            columns_by_id: table_descriptor.columns,
            columns_by_name: SkipMap::new(),
            table_descriptor_file: SimpleDbFileWrapper {file: UnsafeCell::new(table_descriptor_file)},
            next_column_id: AtomicUsize::new(max_column_id as usize + 1),
            table_name: table_descriptor.table_name,
            storage: storage.clone(),
        }))
    }

    pub fn load_tables(
        options: &Arc<shared::SimpleDbOptions>,
        storage: &Arc<storage::Storage>,
    ) -> Result<Vec<Arc<Table>>, SimpleDbError> {
        let mut tables = Vec::new();

        for keysapce_id in storage.get_keyspaces_id() {
            let (descriptor, descriptor_file) = TableDescriptor::load_table_descriptor(options, keysapce_id)?;
            tables.push(Arc::new(Table {
                table_descriptor_file: SimpleDbFileWrapper {file: UnsafeCell::new(descriptor_file)},
                columns_by_name: Self::index_column_id_by_name(&descriptor.columns),
                next_column_id: AtomicUsize::new(descriptor.get_max_column_id() as usize + 1),
                table_name: descriptor.table_name,
                storage_keyspace_id: keysapce_id,
                columns_by_id: descriptor.columns,
                storage: storage.clone(),
            }));
        }

        Ok(tables)
    }

    fn index_column_id_by_name(columns_by_id: &SkipMap<ColumnId, ColumnDescriptor>) -> SkipMap<String, ColumnId> {
        let result = SkipMap::new();
        for entry in columns_by_id.iter() {
            result.insert(entry.value().column_name.clone(), *entry.key());
        }

        result
    }

    fn validate_new_table_columns(
        &self,
        columns: &Vec<(String, ColumnType, bool)>,
    ) -> Result<(), SimpleDbError> {
        let mut primary_already_added = self.has_primary_key();
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
            return Err(NotPrimaryColumnDefined());
        }

        Ok(())
    }

    fn has_primary_key(&self) -> bool {
        self.columns_by_id.iter()
            .find(|i| i.value().is_primary)
            .is_some()
    }

    fn selection_to_columns_id(&self, selection: Selection) -> Result<Vec<ColumnId>, SimpleDbError> {
        match selection {
            Selection::Some(columns_names) => {
                let mut column_ids = Vec::new();

                for column_name in &columns_names {
                    let column_id = self.get_column_id_by_name(column_name)?;
                    column_ids.push(column_id);
                }

                Ok(column_ids)
            },
            Selection::All() => {
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

    pub fn name(&self) -> String {
        self.table_name.clone()
    }
}