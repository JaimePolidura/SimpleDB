use std::collections::HashMap;
use shared::{KeyspaceId, SimpleDbError, SimpleDbFile};
use std::path::PathBuf;
use std::sync::Arc;
use bytes::{Buf, BufMut};

pub struct TableDescriptor {
    columns: HashMap<shared::ColumnId, ColumnDescriptor>,
    file: SimpleDbFile,
}

pub struct ColumnDescriptor {
    column_id: shared::ColumnId,
    column_type: ColumnType,
    name: String,
}

impl TableDescriptor {
    pub fn load_table_descriptor(
        options: &Arc<shared::SimpleDbOptions>,
        keyspace_id: KeyspaceId
    ) -> Result<TableDescriptor, SimpleDbError> {
        let path = Self::column_descriptor_file_path(options, keyspace_id);
        let table_descriptor_file = SimpleDbFile::open(
            path.as_path(),
            shared::SimpleDbFileMode::RandomWrites
        ).map_err(|e| SimpleDbError::CannotOpenTableDescriptor(keyspace_id, e))?;

        let table_descriptor_bytes = table_descriptor_file.read_all()
            .map_err(|e| SimpleDbError::CannotReadTableDescriptor(keyspace_id, e))?;
        let mut column_descriptors = Self::decode_table_descriptor_bytes(
            keyspace_id,
            &table_descriptor_bytes,
            &path
        )?;

        Ok(TableDescriptor{
            columns: Self::index_by_column_name(&mut column_descriptors),
            file: table_descriptor_file,
        })
    }

    fn decode_table_descriptor_bytes(
        keyspace_id: KeyspaceId,
        bytes: &Vec<u8>,
        path: &PathBuf,
    ) -> Result<Vec<ColumnDescriptor>, SimpleDbError> {
        let mut current_ptr = bytes.as_slice();
        let mut columns_descriptor = Vec::new();

        while current_ptr.has_remaining() {
            let column_id = current_ptr.get_u16_le() as shared::ColumnId;
            let column_type = ColumnType::deserialize(current_ptr.get_u8())
                .map_err(|unknown_flag| SimpleDbError::CannotDecodeTableDescriptor(keyspace_id, shared::DecodeError {
                    error_type: shared::DecodeErrorType::UnknownFlag(unknown_flag as usize),
                    index: columns_descriptor.len(),
                    path: path.clone(),
                    offset: 0,
                }))?;
            let name_bytes_length = current_ptr.get_u16_le() as usize;
            let name_bytes = &current_ptr[..name_bytes_length];
            current_ptr.advance(name_bytes_length);

            let column_name = String::from_utf8(name_bytes.to_vec())
                .map_err(|e| shared::SimpleDbError::CannotDecodeTableDescriptor(keyspace_id, shared::DecodeError {
                    error_type: shared::DecodeErrorType::Utf8Decode(e),
                    index: columns_descriptor.len(),
                    path: path.clone(),
                    offset: 0,
                }))?;

            columns_descriptor.push(ColumnDescriptor {
                name: column_name,
                column_type,
                column_id,
            });
        }

        Ok(columns_descriptor)
    }

    fn index_by_column_name(column_descriptors: &mut Vec<ColumnDescriptor>) -> HashMap<shared::ColumnId, ColumnDescriptor> {
        let mut indexed = HashMap::new();

        while let Some(column_descriptor) = column_descriptors.pop() {
            indexed.insert(column_descriptor.column_id, column_descriptor);
        }

        indexed
    }

    fn column_descriptor_file_path(
        options: &Arc<shared::SimpleDbOptions>,
        keyspace_id: KeyspaceId
    ) -> PathBuf {
        let mut path = PathBuf::from(&options.base_path);
        let filename = format!("{}.desc", keyspace_id);
        path.push(filename);
        path
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