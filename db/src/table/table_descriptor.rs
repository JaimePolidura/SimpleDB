use bytes::{Buf, BufMut};
use crossbeam_skiplist::SkipMap;
use shared::{ColumnId, KeyspaceId, SimpleDbError, SimpleDbFile};
use std::cmp::max;
use std::path::PathBuf;
use std::sync::Arc;
use crate::value::Type;

const NO_INDEX: KeyspaceId = 0xFFFFFFFFFFFFFFFF;

//Maintains information about column ID with its column name, column type, is_primary etc.
//This file is stored in binary format
//There is one file of these for each table

// Flags (u64) | Table name length (u16) | Table name bytes...
// [ Column ID (u16) | Column type (u8) | Is primary (u8) | index keyspace ID (u64) | name length (u32) | name bytes... ]
pub struct TableDescriptor {
    pub(crate) columns: SkipMap<ColumnId, ColumnDescriptor>,
    pub(crate) table_name: String,
    pub(crate) primary_column_id: ColumnId,
}

#[derive(Clone, Debug, PartialOrd, PartialEq)]
pub struct ColumnDescriptor {
    pub(crate) column_id: ColumnId,
    pub(crate) column_type: Type,
    pub(crate) column_name: String,
    pub(crate) is_primary: bool,
    pub(crate) secondary_index_keyspace_id: Option<KeyspaceId>,
}

impl TableDescriptor {
    pub fn create(
        keyspace_id: KeyspaceId,
        options: &Arc<shared::SimpleDbOptions>,
        table_name: &str,
    ) -> Result<(TableDescriptor, SimpleDbFile), SimpleDbError> {
        let table_descriptor_file_bytes: Vec<u8> = Self::serialize(Vec::new(), table_name);

        let table_descriptor_file = SimpleDbFile::create(
            Self::table_descriptor_file_path(options, keyspace_id).as_path(),
            &table_descriptor_file_bytes,
            shared::SimpleDbFileMode::AppendOnly
        ).map_err(|e| SimpleDbError::CannotCreateTableDescriptor(keyspace_id, e))?;

        Ok((TableDescriptor {
            table_name: table_name.to_string(),
            primary_column_id: 0,
            columns: SkipMap::new(),
        }, table_descriptor_file))
    }

    pub fn load_from_disk(
        options: &Arc<shared::SimpleDbOptions>,
        keyspace_id: KeyspaceId
    ) -> Result<(TableDescriptor, SimpleDbFile), SimpleDbError> {
        let path = Self::table_descriptor_file_path(options, keyspace_id);
        let table_descriptor_file = SimpleDbFile::open(
            path.as_path(),
            shared::SimpleDbFileMode::AppendOnly
        ).map_err(|e| SimpleDbError::CannotOpenTableDescriptor(keyspace_id, e))?;

        let table_descriptor_bytes = table_descriptor_file.read_all()
            .map_err(|e| SimpleDbError::CannotReadTableDescriptor(keyspace_id, e))?;
        let (table_name, mut column_descriptors, primary_column_id) = Self::deserialize_table_descriptor_bytes(
            keyspace_id,
            &table_descriptor_bytes
        )?;

        Ok((TableDescriptor {
            columns: Self::index_by_column_name(&mut column_descriptors),
            primary_column_id,
            table_name,
        }, table_descriptor_file))
    }

    pub fn serialize(
        columns: Vec<ColumnDescriptor>,
        table_name: &str
    ) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();
        let table_name_bytes = table_name.bytes();
        serialized.put_u32_le(table_name_bytes.len() as u32);
        serialized.extend(table_name_bytes);
        for column in columns {
            serialized.extend(column.serialize());
        }

        serialized
    }

    pub fn get_max_column_id(&self) -> ColumnId {
        if let Some(entry) = self.columns.back() {
            *entry.key()
        } else {
            0
        }
    }

    pub fn get_primary_column_name(&self) -> String {
        self.columns.get(&self.primary_column_id).unwrap()
            .value()
            .column_name
            .clone()
    }

    pub fn name(&self) -> String {
        self.table_name.clone()
    }

    fn max_column_id(column_descriptors: &Vec<ColumnDescriptor>) -> ColumnId {
        let mut max_column_id = 0;
        for column_descriptor in column_descriptors {
            max_column_id = max(column_descriptor.column_id, max_column_id);
        }

        max_column_id
    }

    fn deserialize_table_descriptor_bytes(
        keyspace_id: KeyspaceId,
        bytes: &Vec<u8>,
    ) -> Result<(String, Vec<ColumnDescriptor>, ColumnId), SimpleDbError> {
        let mut current_ptr = bytes.as_slice();
        let mut columns_descriptor = Vec::new();

        //Table name
        let table_name_length = current_ptr.get_u32_le() as usize;
        let name_bytes = &current_ptr[..table_name_length];
        current_ptr.advance(table_name_length);
        let table_name = decode_string(name_bytes, keyspace_id, 0)?;
        let mut primary_column_id = 0;

        while current_ptr.has_remaining() {
            let column_descriptor = ColumnDescriptor::deserialize(
                keyspace_id, columns_descriptor.len(), &mut current_ptr
            )?;

            if column_descriptor.is_primary {
                primary_column_id = column_descriptor.column_id;
            }

            columns_descriptor.push(column_descriptor);
        }

        Ok((table_name, columns_descriptor, primary_column_id))
    }

    fn index_by_column_name(column_descriptors: &mut Vec<ColumnDescriptor>) -> SkipMap<shared::ColumnId, ColumnDescriptor> {
        let mut indexed = SkipMap::new();

        while let Some(column_descriptor) = column_descriptors.pop() {
            indexed.insert(column_descriptor.column_id, column_descriptor);
        }

        indexed
    }

    fn table_descriptor_file_path(
        options: &Arc<shared::SimpleDbOptions>,
        keyspace_id: KeyspaceId
    ) -> PathBuf {
        let mut path = PathBuf::from(&options.base_path);
        let filename = format!("{}.desc", keyspace_id);
        path.push(filename);
        path
    }
}

impl ColumnDescriptor {
    pub fn deserialize(
        keyspace_id: KeyspaceId,
        n_column: usize,
        current_ptr: &mut &[u8]
    ) -> Result<ColumnDescriptor, SimpleDbError> {
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
        let column_name = decode_string(column_bytes, keyspace_id, n_column)?;
        current_ptr.advance(column_name_bytes_length);

        Ok(ColumnDescriptor{
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

fn decode_string(bytes: &[u8], keyspace_id: KeyspaceId, index: usize) -> Result<String, SimpleDbError> {
    String::from_utf8(bytes.to_vec())
        .map_err(|e| shared::SimpleDbError::CannotDecodeTableDescriptor(keyspace_id, shared::DecodeError {
            error_type: shared::DecodeErrorType::Utf8Decode(e),
            offset: 0,
            index,
        }))
}