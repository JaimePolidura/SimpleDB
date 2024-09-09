use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use shared::{SimpleDbError, SimpleDbFile, SimpleDbOptions};
use bytes::{Buf, BufMut};

pub struct DatabaseDescriptor {
    keyspace_id_by_table_name: HashMap<String, shared::KeyspaceId>,
    file: SimpleDbFile,
}

impl DatabaseDescriptor {
    pub fn load_database_descriptor(
        database_options: &Arc<SimpleDbOptions>,
        database_name: &String,
    ) -> Result<DatabaseDescriptor, SimpleDbError> {
        let path = Self::database_descriptor_file_path(&database_options);
        let database_descriptor_file = SimpleDbFile::open(
            path.as_path(), shared::SimpleDbFileMode::RandomWrites
        ).map_err(|e| SimpleDbError::CannotOpenDatabaseDescriptor(String::from(database_name.clone()), e))?;

        let database_descriptor_file_bytes = database_descriptor_file.read_all()
            .map_err(|e| SimpleDbError::CannotOpenDatabaseDescriptor(String::from(database_name.clone()), e))?;

        Self::decode_database_descriptor_bytes(
            database_descriptor_file,
            database_descriptor_file_bytes,
            database_name,
            &path
        )
    }

    fn decode_database_descriptor_bytes(
        file: SimpleDbFile,
        bytes: Vec<u8>,
        database_name: &String,
        path: &PathBuf,
    ) -> Result<DatabaseDescriptor, SimpleDbError> {
        let mut current_ptr = bytes.as_slice();
        let mut keyspace_id_by_table_name = HashMap::new();

        while current_ptr.has_remaining() {
            let database_name_length = current_ptr.get_u32_le() as usize;
            let database_name_bytes = &current_ptr[..database_name_length];
            current_ptr.advance(database_name_length);

            let database_name_string = String::from_utf8(database_name_bytes.to_vec())
                .map_err(|e| shared::SimpleDbError::CannotDecodeDatabaseDescriptor(database_name.clone(), shared::DecodeError {
                    path: path.clone(),
                    offset: 0,
                    index: keyspace_id_by_table_name.len(),
                    error_type: shared::DecodeErrorType::Utf8Decode(e)
                }))?;

            let database_keyspace_id = current_ptr.get_u64_le() as shared::KeyspaceId;

            keyspace_id_by_table_name.insert(database_name_string, database_keyspace_id);
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