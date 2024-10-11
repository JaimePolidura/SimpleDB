use crate::Type::I64;
use crate::Type;
use bytes::{Buf, BufMut};
use crossbeam_skiplist::SkipMap;
use shared::{utils, ColumnId, KeyspaceId, SimpleDbError};
use std::cmp::max;
use std::hash::Hash;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use crossbeam_skiplist::map::Entry;
use shared::SimpleDbError::ColumnNotFound;

const NO_INDEX: KeyspaceId = 0xFFFFFFFFFFFFFFFF;

pub struct Schema {
    primary_column_id: AtomicUsize, //We use atomic, so we can modify it when using non mutable references
    columns_by_id: SkipMap<ColumnId, Column>,
    columns_id_by_name: SkipMap<String, ColumnId>,
}

#[derive(Clone, Debug, PartialOrd, PartialEq)]
pub struct Column {
    pub column_id: ColumnId,
    pub column_type: Type,
    pub column_name: String,
    pub is_primary: bool,
    pub secondary_index_keyspace_id: Option<KeyspaceId>,
}

impl Schema {
    pub fn emtpy() -> Schema {
        Schema {
            primary_column_id: AtomicUsize::new(0),
            columns_id_by_name: SkipMap::new(),
            columns_by_id: SkipMap::new(),
        }
    }

    pub fn create(columns: Vec<Column>) -> Schema {
        let mut columns_by_id = SkipMap::new();
        let mut columns_id_by_name = SkipMap::new();
        let mut primary_column_id = 0;

        for column in columns {
            if column.is_primary {
                primary_column_id = column.column_id.clone();
            }
            columns_id_by_name.insert(column.column_name.clone(), column.column_id.clone());
            columns_by_id.insert(column.column_id.clone(), column);
        }

        Schema {
            primary_column_id: AtomicUsize::new(primary_column_id as usize),
            columns_id_by_name,
            columns_by_id,
        }
    }

    pub(crate) fn add_column(
        &self,
        column: Column,
    ) {
        self.columns_by_id.insert(column.column_id, column.clone());
        self.columns_id_by_name.insert(column.column_name, column.column_id);
    }

    pub(crate) fn update_column_secondary_index(
        &self,
        column_id: ColumnId,
        secondary_index_keyspace_id: KeyspaceId
    ) {
        let mut column_to_update = self.columns_by_id.remove(&column_id)
            .unwrap()
            .value()
            .clone();

        column_to_update.secondary_index_keyspace_id = Some(secondary_index_keyspace_id);

        self.columns_by_id.insert(column_id, column_to_update);
    }

    pub fn get_columns(&self) -> Vec<Column> {
        let mut columns = Vec::new();
        for column in self.columns_by_id.iter() {
            columns.push(column.value().clone());
        }
        columns
    }

    pub fn get_column(&self, name: &str) -> Option<Column> {
        match self.columns_id_by_name.get(name) {
            Some(column_id) => {
                let column = self.columns_by_id.get(column_id.value()).unwrap();
                let column = column.value();
                Some(column.clone())
            },
            None => None
        }
    }

    pub fn get_column_or_err(&self, column_name: &str) -> Result<Column, SimpleDbError> {
        match self.get_column(&column_name) {
            Some(desc) => Ok(desc),
            None => Err(ColumnNotFound(column_name.to_string()))
        }
    }

    pub fn get_secondary_indexed_columns(&self) -> Vec<Column> {
        let mut columns = Vec::new();
        for column in self.columns_by_id.iter() {
            let column = column.value();
            if column.is_secondary_indexed() && !column.is_primary {
                columns.push(column.clone());
            }
        }

        columns
    }

    pub fn get_primary_column(&self) -> Column {
        self.columns_by_id.get(&(self.primary_column_id.load(Relaxed) as ColumnId)).unwrap().value().clone()
    }

    pub fn get_max_column_id(&self) -> ColumnId {
        let mut max_column_id = 0;
        for entry in self.columns_by_id.iter() {
            max_column_id = max(max_column_id, *entry.key());
        }

        max_column_id
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();
        serialized.put_u32_le(self.columns_by_id.len() as u32);
        for entry in self.columns_by_id.iter() {
            let column = entry.value();
            serialized.extend(column.serialize());
        }

        serialized
    }

    pub fn is_secondary_indexed(&self, column_name: &str) -> bool {
        match self.columns_id_by_name.get(column_name) {
            Some(column_id) => {
                let column = self.columns_by_id.get(column_id.value())
                    .unwrap();
                let column = column.value();

                column.is_secondary_indexed()
            },
            None => false,
        }
    }

    pub fn deserialize(ptr: &mut &[u8], keyspace_id: KeyspaceId) -> Result<Schema, SimpleDbError> {
        let mut columns = Vec::new();
        while ptr.has_remaining() {
            columns.push(Column::deserialize(keyspace_id, columns.len(), ptr)?);
        }

        Ok(Schema::create(columns))
    }
}

impl Clone for Schema {
    fn clone(&self) -> Self {
        Schema {
            primary_column_id: AtomicUsize::new(self.primary_column_id.load(Relaxed)),
            columns_id_by_name: utils::clone_skipmap(&self.columns_id_by_name),
            columns_by_id: utils::clone_skipmap(&self.columns_by_id),
        }
    }
}

impl Column {
    pub fn create_primary(name: &str) -> Column {
        Column {
            column_id: 0,
            column_type: I64,
            column_name: name.to_string(),
            is_primary: true,
            secondary_index_keyspace_id: None
        }
    }

    pub fn create_secondary(name: &str, column_id: ColumnId) -> Column {
        Column {
            column_id,
            column_type: I64,
            column_name: name.to_string(),
            is_primary: false,
            secondary_index_keyspace_id: Some(1)
        }
    }

    pub fn create(name: &str, column_id: ColumnId) -> Column {
        Column {
            secondary_index_keyspace_id: None,
            column_name: name.to_string(),
            is_primary: false,
            column_type: I64,
            column_id,
        }
    }

    pub fn deserialize(
        keyspace_id: KeyspaceId,
        n_column: usize,
        current_ptr: &mut &[u8]
    ) -> Result<Column, SimpleDbError> {
        let column_id = current_ptr.get_u16_le() as shared::ColumnId;
        let column_type = Type::deserialize(current_ptr.get_u8())
            .map_err(|unknown_flag| SimpleDbError::CannotDecodeTableDescriptor(keyspace_id, shared::DecodeError {
                error_type: shared::DecodeErrorType::UnknownFlag(unknown_flag as usize),
                index: n_column,
                offset: 0,
            }))?;
        let is_primary = current_ptr.get_u8() != 0;
        let secondary_index_keyspace_id = Self::get_secondary_index_keyspace_id(current_ptr.get_u64_le());
        let column_name_bytes_length = current_ptr.get_u32_le() as usize;
        let column_bytes = &current_ptr[..column_name_bytes_length];
        let column_name = String::from_utf8(column_bytes.to_vec())
            .map_err(|e| shared::SimpleDbError::CannotDecodeTableDescriptor(keyspace_id, shared::DecodeError {
                error_type: shared::DecodeErrorType::Utf8Decode(e),
                offset: 0,
                index: n_column,
            }))?;

        current_ptr.advance(column_name_bytes_length);

        Ok(Column{
            secondary_index_keyspace_id,
            column_name,
            column_type,
            is_primary,
            column_id,
        })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.put_u16_le(self.column_id);
        serialized.put_u8(self.column_type.serialize());
        serialized.put_u8(self.is_primary as u8);
        serialized.put_u64_le(self.get_index_keyspace() as u64);
        let name_bytes = self.column_name.bytes();
        serialized.put_u32_le(name_bytes.len() as u32);
        serialized.extend(name_bytes);
        serialized
    }

    pub fn get_secondary_index_keyspace_id(value: u64) -> Option<KeyspaceId> {
        if value as KeyspaceId != NO_INDEX {
            Some(value as KeyspaceId)
        } else {
            None
        }
    }

    pub fn get_index_keyspace(&self) -> KeyspaceId {
        self.secondary_index_keyspace_id.unwrap_or_else(|| NO_INDEX as KeyspaceId)
    }

    pub fn is_secondary_indexed(&self) -> bool {
        self.secondary_index_keyspace_id.is_some()
    }
}