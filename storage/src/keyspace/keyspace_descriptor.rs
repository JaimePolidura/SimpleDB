use bytes::{Buf, BufMut};
use shared::SimpleDbError::{CannotCreateKeyspaceDescriptorFile, CannotDecodeKeyspaceDescriptor, CannotOpenKeyspaceDescriptorFile, CannotReadKeyspaceDescriptorFile};
use shared::{DecodeError, DecodeErrorType, Flag, KeyspaceId, SimpleDbError, SimpleDbFile, SimpleDbFileMode, Type};
use std::path::PathBuf;

#[derive(Copy, Clone)]
pub struct KeyspaceDescriptor {
    pub(crate) flags: Flag,
    pub(crate) key_type: Type,
    pub(crate) keyspace_id: KeyspaceId //Not serialized
}

impl KeyspaceDescriptor {
    pub fn create_mock(
        key_type: Type
    ) -> KeyspaceDescriptor {
        KeyspaceDescriptor {
            keyspace_id: 0,
            key_type,
            flags: 0,
        }
    }

    pub fn create(
        flags: Flag,
        keyspace_path: PathBuf,
        keyspace_id: KeyspaceId,
        key_type: Type
    ) -> Result<KeyspaceDescriptor, SimpleDbError> {
        let keyspace_descriptor = KeyspaceDescriptor {
            keyspace_id,
            key_type,
            flags,
        };

        let keyspace_descriptor_path = Self::to_keyspace_path(keyspace_path);
        let keyspace_descriptor_bytes = keyspace_descriptor.serialize();

        SimpleDbFile::create(keyspace_descriptor_path.as_path(), &keyspace_descriptor_bytes, SimpleDbFileMode::RandomWrites)
            .map_err(|e| CannotCreateKeyspaceDescriptorFile(keyspace_id, e))?;

        Ok(keyspace_descriptor)
    }

    pub fn load_from_disk(
        keyspace_id: KeyspaceId,
        keyspace_path: PathBuf,
    )  -> Result<KeyspaceDescriptor, SimpleDbError> {
        let path = Self::to_keyspace_path(keyspace_path);
        let keyspace_file = SimpleDbFile::open(path.as_path(), SimpleDbFileMode::RandomWrites)
            .map_err(|e| CannotReadKeyspaceDescriptorFile(keyspace_id, e))?;
        let keyspace_desc_bytes = keyspace_file.read_all()
            .map_err(|e| CannotOpenKeyspaceDescriptorFile(keyspace_id, e))?;

        Self::deserialize(&mut keyspace_desc_bytes.as_slice(), keyspace_id)
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();
        serialized.put_u8(self.key_type.serialize() as u8);
        serialized.put_u64_le(self.flags as u64);
        serialized
    }

    pub fn deserialize(bytes: &mut &[u8], keyspace_id: KeyspaceId) -> Result<KeyspaceDescriptor, SimpleDbError> {
        let key_type = Type::deserialize(bytes.get_u8())
            .map_err(|unknown_flag| CannotDecodeKeyspaceDescriptor(keyspace_id, DecodeError{
                offset: 0,
                index: 0,
                error_type: DecodeErrorType::UnknownFlag(unknown_flag as usize)
            }))?;
        let flags = bytes.get_u64_le();

        Ok(KeyspaceDescriptor {
            keyspace_id,
            key_type,
            flags
        })
    }

    fn to_keyspace_path(mut keyspace_path: PathBuf) -> PathBuf {
        keyspace_path.push("desc");
        keyspace_path
    }
}
