use bytes::{Buf, BufMut};
use crossbeam_skiplist::SkipMap;
use shared::{ColumnId, KeyspaceId, SimpleDbError, SimpleDbFile};
use std::cmp::max;
use std::path::PathBuf;
use std::sync::Arc;

pub struct TableDescriptor {
    pub(crate) columns: SkipMap<ColumnId, ColumnDescriptor>,
    pub(crate) table_name: String,
}

pub struct ColumnDescriptor {
    pub(crate) column_id: ColumnId,
    pub(crate) column_type: ColumnType,
    pub(crate) column_name: String,
    pub(crate) is_primary: bool,
}

impl TableDescriptor {
    pub fn get_max_column_id(&self) -> ColumnId {
        if let Some(entry) = self.columns.back() {
            *entry.key()
        } else {
            0
        }
    }

    pub fn name(&self) -> String {
        self.table_name.clone()
    }

    pub fn create(
        keyspace_id: KeyspaceId,
        options: &Arc<shared::SimpleDbOptions>,
        table_name: &str
    ) -> Result<(TableDescriptor, SimpleDbFile), SimpleDbError> {
        let mut table_descriptor_file_bytes: Vec<u8> = Vec::new();
        let table_name_butes = table_name.bytes();
        table_descriptor_file_bytes.put_u16_le(table_name_butes.len() as u16);
        table_descriptor_file_bytes.extend(table_name_butes);

        let table_descriptor_file = SimpleDbFile::create(
            Self::table_descriptor_file_path(options, keyspace_id).as_path(),
            &table_descriptor_file_bytes,
            shared::SimpleDbFileMode::AppendOnly
        ).map_err(|e| SimpleDbError::CannotCreateTableDescriptor(keyspace_id, e))?;

        Ok((TableDescriptor {
            table_name: table_name.to_string(),
            columns: SkipMap::new(),
        }, table_descriptor_file))
    }

    pub fn load_table_descriptor(
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
        let (table_name, mut column_descriptors) = Self::decode_table_descriptor_bytes(
            keyspace_id,
            &table_descriptor_bytes,
            &path
        )?;

        Ok((TableDescriptor {
            columns: Self::index_by_column_name(&mut column_descriptors),
            table_name,
        }, table_descriptor_file))
    }

    fn max_column_id(column_descriptors: &Vec<ColumnDescriptor>) -> ColumnId {
        let mut max_column_id = 0;
        for column_descriptor in column_descriptors {
            max_column_id = max(column_descriptor.column_id, max_column_id);
        }

        max_column_id
    }

    fn decode_table_descriptor_bytes(
        keyspace_id: KeyspaceId,
        bytes: &Vec<u8>,
        path: &PathBuf,
    ) -> Result<(String, Vec<ColumnDescriptor>), SimpleDbError> {
        let mut current_ptr = bytes.as_slice();
        let mut columns_descriptor = Vec::new();

        //Table name
        let table_name_length = current_ptr.get_u16_le() as usize;
        let name_bytes = &current_ptr[..table_name_length];
        current_ptr.advance(table_name_length);
        let table_name = Self::decode_string(name_bytes, keyspace_id, path, 0)?;

        while current_ptr.has_remaining() {
            let column_id = current_ptr.get_u16_le() as shared::ColumnId;
            let column_type = ColumnType::deserialize(current_ptr.get_u8())
                .map_err(|unknown_flag| SimpleDbError::CannotDecodeTableDescriptor(keyspace_id, shared::DecodeError {
                    error_type: shared::DecodeErrorType::UnknownFlag(unknown_flag as usize),
                    index: columns_descriptor.len(),
                    path: path.clone(),
                    offset: 0,
                }))?;
            let is_primary = current_ptr.get_u8() != 0;
            let name_bytes_length = current_ptr.get_u16_le() as usize;
            let name_bytes = &current_ptr[..name_bytes_length];
            current_ptr.advance(name_bytes_length);

            let column_name = Self::decode_string(name_bytes, keyspace_id, path, columns_descriptor.len())?;

            columns_descriptor.push(ColumnDescriptor {
                column_name,
                column_type,
                column_id,
                is_primary
            });
        }

        Ok((table_name, columns_descriptor))
    }

    fn decode_string(bytes: &[u8], keyspace_id: KeyspaceId, path: &PathBuf, index: usize) -> Result<String, SimpleDbError> {
        String::from_utf8(bytes.to_vec())
            .map_err(|e| shared::SimpleDbError::CannotDecodeTableDescriptor(keyspace_id, shared::DecodeError {
                error_type: shared::DecodeErrorType::Utf8Decode(e),
                index: index,
                path: path.clone(),
                offset: 0,
            }))
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
    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.put_u16_le(self.column_id);
        serialized.put_u8(self.column_type.serialize());
        serialized.put_u8(self.is_primary as u8);
        let name_bytes = self.column_name.bytes();
        serialized.put_u64_le(name_bytes.len() as u64);
        serialized.extend(name_bytes);
        serialized
    }
}

pub enum ColumnType {
    I8,
    U8,
    I16,
    U16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    BOOLEAN,
    TEXT,
    DATE,
    BLOB
}

impl ColumnType {
    pub fn serialize(&self) -> u8 {
        match *self {
            ColumnType::I8 => 1,
            ColumnType::U8 => 2,
            ColumnType::I16 => 3,
            ColumnType::U16 => 4,
            ColumnType::U32 => 5,
            ColumnType::I32 => 6,
            ColumnType::U64 => 7,
            ColumnType::I64 => 8,
            ColumnType::F32 => 9,
            ColumnType::F64 => 10,
            ColumnType::BOOLEAN => 11,
            ColumnType::TEXT => 12,
            ColumnType::DATE => 13,
            ColumnType::BLOB => 14,
        }
    }

    pub fn deserialize(value: u8) -> Result<ColumnType, u8> {
        match value {
            1 =>  Ok(ColumnType::I8),
            2 =>  Ok(ColumnType::U8),
            3 =>  Ok(ColumnType::I16),
            4 =>  Ok(ColumnType::U16),
            5 =>  Ok(ColumnType::U32),
            6 =>  Ok(ColumnType::I32),
            7 =>  Ok(ColumnType::U64),
            8 =>  Ok(ColumnType::I64),
            9 =>  Ok(ColumnType::F32),
            10 => Ok(ColumnType::F64) ,
            11 => Ok(ColumnType::BOOLEAN),
            12 => Ok(ColumnType::TEXT),
            13 => Ok(ColumnType::DATE),
            14 => Ok(ColumnType::BLOB),
            _ => Err((value))
        }
    }
}