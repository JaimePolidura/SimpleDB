use crate::table::schema::{Column, Schema};
use bytes::{Buf, BufMut};
use shared::SimpleDbError::CannotWriteTableDescriptor;
use shared::{ColumnId, KeyspaceId, SimpleDbError, SimpleDbFile, Type};
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex};

//Maintains information about column ID with its column name, column type, is_primary etc.
//This file is stored in binary format
//There is one file of these for each table
pub struct TableDescriptor {
    pub(crate) file: Mutex<SimpleDbFile>,
    pub(crate) table_name: String,
    pub(crate) schema: Schema,
    pub(crate) next_column_id: AtomicUsize,
    pub(crate) storage_keyspace_id: KeyspaceId
}

impl TableDescriptor {
    pub fn create(
        keyspace_id: KeyspaceId,
        options: &Arc<shared::SimpleDbOptions>,
        table_name: &str,
        columns: Vec<(String, Type, bool)>,
    ) -> Result<TableDescriptor, SimpleDbError> {
        let mut next_column_id = AtomicUsize::new(0);

        let mut table_descriptor = TableDescriptor {
            file: Mutex::new(SimpleDbFile::create_mock()),
            next_column_id: AtomicUsize::new(0),
            table_name: table_name.to_string(),
            storage_keyspace_id: keyspace_id,
            schema: Schema::create(columns.iter()
                .map(|(column_name, column_type, is_primary)| {
                    Column {
                        column_id: next_column_id.fetch_add(1, Relaxed) as ColumnId,
                        column_type: column_type.clone(),
                        column_name: column_name.clone(),
                        is_primary: *is_primary,
                        secondary_index_keyspace_id: None,
                    }
                })
                .collect()),
        };

        let table_descriptor_file = SimpleDbFile::create(
            Self::table_descriptor_file_path(options, keyspace_id).as_path(),
            &table_descriptor.serialize(),
            shared::SimpleDbFileMode::RandomWrites
        ).map_err(|e| SimpleDbError::CannotCreateTableDescriptor(keyspace_id, e))?;

        table_descriptor.file = Mutex::new(table_descriptor_file);
        table_descriptor.next_column_id = next_column_id;

        Ok(table_descriptor)
    }

    pub fn create_mock(columns: Vec<Column>) -> TableDescriptor {
        TableDescriptor {
            file: Mutex::new(SimpleDbFile::create_mock()),
            table_name: String::from(""),
            schema: Schema::create(columns),
            next_column_id: AtomicUsize::new(10),
            storage_keyspace_id: 0,
        }
    }

    pub fn load_from_disk(
        options: &Arc<shared::SimpleDbOptions>,
        keyspace_id: KeyspaceId
    ) -> Result<TableDescriptor, SimpleDbError> {
        let path = Self::table_descriptor_file_path(options, keyspace_id);
        let table_descriptor_file = SimpleDbFile::open(
            path.as_path(),
            shared::SimpleDbFileMode::RandomWrites
        ).map_err(|e| SimpleDbError::CannotOpenTableDescriptor(keyspace_id, e))?;

        let table_descriptor_bytes = table_descriptor_file.read_all()
            .map_err(|e| SimpleDbError::CannotReadTableDescriptor(keyspace_id, e))?;

        let mut table_descriptor = Self::deserialize(
            keyspace_id,
            &table_descriptor_bytes
        )?;
        table_descriptor.file = Mutex::new(table_descriptor_file);

        Ok(table_descriptor)
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn add_column(
        &self,
        name: &str,
        column_type: Type,
        is_primary: bool
    ) -> Result<(), SimpleDbError> {
        self.schema.add_column(Column {
            column_id: self.next_column_id.fetch_add(1, Relaxed) as ColumnId,
            secondary_index_keyspace_id: None,
            column_name: name.to_string(),
            column_type,
            is_primary,
        });

        let mut file = self.file.lock().unwrap();
        file.safe_replace(&self.serialize())
            .map_err(|e| SimpleDbError::CannotWriteTableDescriptor(self.storage_keyspace_id, e))?;

        Ok(())
    }

    pub fn update_column_secondary_index(
        &self,
        column_id_indexed: ColumnId,
        keyspace_id: KeyspaceId
    ) -> Result<(), SimpleDbError> {
        let mut file_lock = self.file.lock().unwrap();
        let mut new_columns = Vec::new();
        let columns = self.schema.get_columns();
        for current in columns {
            if current.column_id == column_id_indexed {
                let mut updated_column = current.clone();
                updated_column.secondary_index_keyspace_id = Some(keyspace_id);
                new_columns.push(updated_column);
            } else {
                new_columns.push(current);
            }
        }

        self.schema.update_column_secondary_index(
            column_id_indexed,
            keyspace_id
        );

        let serialized = self.serialize();
        file_lock.safe_replace(&serialized)
            .map_err(|io_error| CannotWriteTableDescriptor(self.storage_keyspace_id, io_error))?;


        Ok(())
    }

    pub fn serialize(
        &self
    ) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();
        let table_name_bytes = self.table_name.bytes();
        serialized.put_u32_le(table_name_bytes.len() as u32);
        serialized.extend(table_name_bytes);
        serialized.extend(self.schema.serialize());

        serialized
    }

    fn deserialize(
        storage_keyspace_id: KeyspaceId,
        bytes: &Vec<u8>,
    ) -> Result<TableDescriptor, SimpleDbError> {
        let mut current_ptr = bytes.as_slice();

        //Table name
        let table_name_length = current_ptr.get_u32_le() as usize;
        let name_bytes = &current_ptr[..table_name_length];
        current_ptr.advance(table_name_length);
        let table_name = decode_string(name_bytes, storage_keyspace_id, 0)?;

        let schema = Schema::deserialize(&mut current_ptr, storage_keyspace_id)?;

        Ok(TableDescriptor {
            next_column_id: AtomicUsize::new(schema.get_max_column_id() as usize + 1),
            file: Mutex::new(SimpleDbFile::create_mock()), //Temporal
            storage_keyspace_id,
            table_name,
            schema
        })
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

fn decode_string(bytes: &[u8], keyspace_id: KeyspaceId, index: usize) -> Result<String, SimpleDbError> {
    String::from_utf8(bytes.to_vec())
        .map_err(|e| shared::SimpleDbError::CannotDecodeTableDescriptor(keyspace_id, shared::DecodeError {
            error_type: shared::DecodeErrorType::Utf8Decode(e),
            offset: 0,
            index,
        }))
}