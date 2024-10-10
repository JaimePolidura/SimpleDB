use bytes::{Buf, BufMut};
use crossbeam_skiplist::SkipMap;
use shared::SimpleDbError::{CannotCreateDatabaseDescriptor, CannotWriteDatabaseDescriptor};
use shared::{KeyspaceId, SimpleDbError, SimpleDbFile, SimpleDbFileMode, SimpleDbOptions};
use std::path::PathBuf;
use std::sync::Arc;

//Contains a mapping between storage engine's keyspace IDs and table names
//This file is stored in binary format
//There is one file of these for each database

// [ Table name length (u32) | Table name bytes... | Keyspace ID (u64) ]
pub(crate) struct DatabaseDescriptor {
    keyspace_id_by_table_name: SkipMap<String, shared::KeyspaceId>,
    file: SimpleDbFile,
}

impl DatabaseDescriptor {
    pub fn mock() -> DatabaseDescriptor {
        DatabaseDescriptor {
            keyspace_id_by_table_name: SkipMap::new(),
            file: SimpleDbFile::mock()
        }
    }

    pub fn create(
        database_options: &Arc<SimpleDbOptions>,
        database_name: &String,
    ) -> Result<DatabaseDescriptor, SimpleDbError> {
        let file = SimpleDbFile::create(
            Self::database_descriptor_file_path(database_options).as_path(),
            &Vec::new(),
            SimpleDbFileMode::AppendOnly
        ).map_err(|error| CannotCreateDatabaseDescriptor(database_name.clone(), error))?;

        Ok(DatabaseDescriptor {
            keyspace_id_by_table_name: SkipMap::new(),
            file,
        })
    }

    pub fn load_database_descriptor(
        database_options: &Arc<SimpleDbOptions>,
        database_name: &String,
    ) -> Result<DatabaseDescriptor, SimpleDbError> {
        let path = Self::database_descriptor_file_path(&database_options);
        let database_descriptor_file = SimpleDbFile::open(
            path.as_path(),
            shared::SimpleDbFileMode::AppendOnly
        ).map_err(|e| SimpleDbError::CannotOpenDatabaseDescriptor(String::from(database_name.clone()), e))?;

        let database_descriptor_file_bytes = database_descriptor_file.read_all()
            .map_err(|e| SimpleDbError::CannotOpenDatabaseDescriptor(String::from(database_name.clone()), e))?;

        Self::deserialize_database_descriptor(
            database_descriptor_file,
            database_descriptor_file_bytes,
            database_name)
    }

    pub fn add_table(&mut self, table_name: &str, keyspace_id: KeyspaceId) -> Result<(), SimpleDbError> {
        self.keyspace_id_by_table_name.insert(table_name.to_string(), keyspace_id);
        let bytes = self.serialize_new_table_entry(table_name, keyspace_id);
        self.file.write(&bytes)
            .map_err(|e| CannotWriteDatabaseDescriptor(e))?;
        Ok(())
    }

    fn serialize_new_table_entry(&self, table_name: &str, keyspace_id: KeyspaceId) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.put_u32_le(table_name.len() as u32);
        serialized.extend(table_name.as_bytes());
        serialized.put_u64_le(keyspace_id as u64);
        serialized
    }

    fn deserialize_database_descriptor(
        file: SimpleDbFile,
        bytes: Vec<u8>,
        database_name: &String,
    ) -> Result<DatabaseDescriptor, SimpleDbError> {
        let mut current_ptr = bytes.as_slice();
        let keyspace_id_by_table_name = SkipMap::new();

        while current_ptr.has_remaining() {
            let table_name_length = current_ptr.get_u32_le() as usize;
            let table_name_bytes = &current_ptr[..table_name_length];
            current_ptr.advance(table_name_length);

            let table_name_string = String::from_utf8(table_name_bytes.to_vec())
                .map_err(|e| SimpleDbError::CannotDecodeDatabaseDescriptor(database_name.clone(), shared::DecodeError {
                    error_type: shared::DecodeErrorType::Utf8Decode(e),
                    index: keyspace_id_by_table_name.len(),
                    offset: 0,
                }))?;

            let database_keyspace_id = current_ptr.get_u64_le() as shared::KeyspaceId;

            keyspace_id_by_table_name.insert(table_name_string, database_keyspace_id);
        }

        Ok(DatabaseDescriptor {
            keyspace_id_by_table_name,
            file
        })
    }

    fn database_descriptor_file_path(
        database_options: &Arc<SimpleDbOptions>,
    ) -> PathBuf {
        let mut pathbuf = PathBuf::from(&database_options.base_path.as_str());
        pathbuf.push("desc");
        pathbuf
    }
}