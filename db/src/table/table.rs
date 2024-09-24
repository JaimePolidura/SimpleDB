use crate::selection::Selection;
use crate::table::record::Record;
use crate::table::row::Row;
use crate::table::table_descriptor::{ColumnDescriptor, TableDescriptor};
use crate::table::table_iteartor::TableIterator;
use crate::ColumnType;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use shared::SimpleDbError::{ColumnNameAlreadyDefined, InvalidType, OnlyOnePrimaryColumnAllowed, PrimaryColumnNotIncluded, UnknownColumn};
use shared::{ColumnId, SimpleDbError, SimpleDbFileWrapper};
use std::cell::UnsafeCell;
use std::collections::{HashMap, HashSet};
use std::hash::Hasher;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use storage::transactions::transaction::Transaction;

pub struct Table {
    pub(crate) storage_keyspace_id: shared::KeyspaceId,
    pub(crate) table_name: String,

    pub(crate) table_descriptor_file: SimpleDbFileWrapper,

    pub(crate) columns_by_id: SkipMap<ColumnId, ColumnDescriptor>,
    pub(crate) columns_by_name: SkipMap<String, shared::ColumnId>,
    pub(crate) next_column_id: AtomicUsize,
    pub(crate) primary_column_name: String,

    pub(crate) storage: Arc<storage::Storage>,
}

impl Table {
    pub fn add_columns(
        &self,
        columns_to_add: Vec<(String, ColumnType, bool)>,
    ) -> Result<(), SimpleDbError> {
        for (column_name, column_type, is_primary) in columns_to_add {
            self.add_column(&column_name, column_type, is_primary)?
        }
        Ok(())
    }

    pub fn get_by_primary_column(
        self: Arc<Self>,
        key: &Bytes,
        transaction: &Transaction,
        selection: Selection
    ) -> Result<Option<Row>, SimpleDbError> {
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
    ) -> Result<TableIterator, SimpleDbError> {
        let selection = self.selection_to_columns_id(selection)?;
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
    ) -> Result<TableIterator, SimpleDbError> {
        let selection = self.selection_to_columns_id(selection)?;
        let storage_iterator = self.storage.scan_all_with_transaction(transaction, self.storage_keyspace_id)?;

        Ok(TableIterator::create(
            storage_iterator,
            selection,
            self.clone()
        ))
    }

    //Expect call to validate_insert before calling this function
    pub fn insert(
        &self,
        transaction: &Transaction,
        to_insert_data: &mut Vec<(String, Bytes)>
    ) -> Result<(), SimpleDbError> {
        let id_value = self.extract_primary_value(to_insert_data).unwrap();
        self.update(transaction, id_value, to_insert_data)
    }

    fn has_primary_value(&self, data: &Vec<(String, Bytes)>) -> bool {
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
                for column_name in selection {
                    if !self.columns_by_name.contains_key(column_name) {
                        return Err(SimpleDbError::ColumnNotFound(self.storage_keyspace_id, column_name.clone()));
                    }
                }

                Ok(())
            }
        }
    }

    pub fn validate_column_values(
        &self,
        to_insert_data: &Vec<(String, Bytes)>
    ) -> Result<(), SimpleDbError> {
        if !self.has_primary_value(to_insert_data) {
            return Err(PrimaryColumnNotIncluded())
        }
        for (column_name, column_value) in to_insert_data {
            match self.columns_by_name.get(column_name) {
                Some(column) => {
                    let column = self.columns_by_id.get(column.value()).unwrap();
                    let column = column.value();

                    if !column.column_type.has_valid_format(column_value) {
                        return Err(InvalidType(column_name.clone()));
                    }
                },
                None => return Err(UnknownColumn(column_name.clone())),
            }
        }

        Ok(())
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

    pub(crate) fn create(
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
            table_descriptor_file: SimpleDbFileWrapper {file: UnsafeCell::new(table_descriptor_file)},
            next_column_id: AtomicUsize::new(max_column_id as usize + 1),
            primary_column_name: table_descriptor.get_primary_column_name(),
            columns_by_id: table_descriptor.columns,
            table_name: table_descriptor.table_name,
            storage_keyspace_id: table_keyspace_id,
            columns_by_name: SkipMap::new(),
            storage: storage.clone(),
        }))
    }

    pub(crate) fn load_tables(
        options: &Arc<shared::SimpleDbOptions>,
        storage: &Arc<storage::Storage>,
    ) -> Result<Vec<Arc<Table>>, SimpleDbError> {
        let mut tables = Vec::new();

        for keysapce_id in storage.get_keyspaces_id() {
            let (descriptor, descriptor_file) = TableDescriptor::load_table_descriptor(options, keysapce_id)?;
            tables.push(Arc::new(Table {
                table_descriptor_file: SimpleDbFileWrapper {file: UnsafeCell::new(descriptor_file)},
                next_column_id: AtomicUsize::new(descriptor.get_max_column_id() as usize + 1),
                columns_by_name: Self::index_column_id_by_name(&descriptor.columns),
                primary_column_name: descriptor.get_primary_column_name(),
                table_name: descriptor.table_name,
                storage_keyspace_id: keysapce_id,
                columns_by_id: descriptor.columns,
                storage: storage.clone(),
            }));
        }

        Ok(tables)
    }

    pub fn get_primary_column_data(&self) -> Option<ColumnDescriptor> {
        match self.columns_by_name.get(&self.primary_column_name) {
            Some(id) => {
                let value = self.columns_by_id.get(id.value()).unwrap();
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

    pub fn name(&self) -> String {
        self.table_name.clone()
    }
}