use std::path::PathBuf;
use bytes::{Buf, BufMut};
use shared::{Flag, KeyspaceId, SimpleDbError, SimpleDbFile, SimpleDbFileMode};
use shared::SimpleDbError::{CannotCreateKeyspaceDescriptorFile, CannotOpenKeyspaceDescriptorFile, CannotReadKeyspaceDescriptorFile};

pub struct KeyspaceDescriptor {
    pub(crate) flags: Flag,
}

impl KeyspaceDescriptor {
    pub fn create(
        flags: Flag,
        keyspace_path: PathBuf,
        keyspace_id: KeyspaceId
    ) -> Result<KeyspaceDescriptor, SimpleDbError> {
        let keyspace_path = Self::to_keyspace_path(keyspace_path);
        SimpleDbFile::create(keyspace_path.as_path(), &flags.to_le_bytes().to_vec(), SimpleDbFileMode::RandomWrites)
            .map_err(|e| CannotCreateKeyspaceDescriptorFile(keyspace_id, e))?;
        Ok(KeyspaceDescriptor{ flags })
    }

    pub fn load_from_disk(
        keyspace_id: KeyspaceId,
        keyspace_path: PathBuf,
    )  -> Result<KeyspaceDescriptor, SimpleDbError> {
        let path = Self::to_keyspace_path(keyspace_path);
        let keyspace_file = SimpleDbFile::open(path.as_path(), SimpleDbFileMode::ReadOnly)
            .map_err(|e| CannotReadKeyspaceDescriptorFile(keyspace_id, e))?;
        let keyspace_desc_bytes = keyspace_file.read_all()
            .map_err(|e| CannotOpenKeyspaceDescriptorFile(keyspace_id, e))?;
        Ok(Self::deserialize(keyspace_desc_bytes))
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut vec: Vec<u8> = Vec::new();
        vec.put_u64_le(self.flags);
        vec
    }

    pub fn deserialize(bytes: Vec<u8>) -> KeyspaceDescriptor {
        let mut bytes_ptr = &mut bytes.as_slice();

        KeyspaceDescriptor {
            flags: bytes_ptr.get_u64_le()
        }
    }

    fn to_keyspace_path(mut keyspace_path: PathBuf) -> PathBuf {
        keyspace_path.push("desc");
        keyspace_path
    }
}