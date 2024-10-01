use crate::key;
use crate::key::Key;
use bytes::{Buf, BufMut, Bytes};
use std::cmp::max;
use std::fs;
use std::fs::DirEntry;
use std::path::PathBuf;
use std::sync::Arc;

pub struct Wal {
    keyspace_id: shared::KeyspaceId,
    options: Arc<shared::SimpleDbOptions>,
    memtable_id: shared::MemtableId,
    file: shared::SimpleDbFile,
}

pub(crate) struct WalEntry {
    pub key: Key,
    pub value: Bytes
}

impl Wal {
    pub fn create(
        options: Arc<shared::SimpleDbOptions>,
        keyspace_id: shared::KeyspaceId,
        memtable_id: shared::MemtableId
    ) -> Result<Wal, shared::SimpleDbError> {
        Ok(Wal {
            file: shared::SimpleDbFile::open(Self::to_wal_file_path(&options, memtable_id, keyspace_id).as_path(), shared::SimpleDbFileMode::AppendOnly)
                .map_err(|e| shared::SimpleDbError::CannotCreateWal(keyspace_id, memtable_id, e))?,
            options,
            keyspace_id,
            memtable_id,
        })
    }

    pub fn create_mock(
        options: Arc<shared::SimpleDbOptions>,
        memtable_id: shared::MemtableId
    ) -> Result<Wal, shared::SimpleDbError> {
        Ok(Wal {
            file: shared::SimpleDbFile::mock(),
            keyspace_id: 0,
            options,
            memtable_id,
        })
    }

    pub fn add_entry(&mut self, key: &Key, value: &[u8]) -> Result<(), shared::SimpleDbError> {
        let encoded = self.encode(key, value);
        self.file.write(&encoded)
            .map_err(|e| shared::SimpleDbError::CannotWriteWalEntry(self.keyspace_id, self.memtable_id, e))?;

        if matches!(self.options.durability_level, shared::DurabilityLevel::Strong) {
            self.file.fsync();
        }

        Ok(())
    }

    pub fn read_entries(&self) -> Result<Vec<WalEntry>, shared::SimpleDbError> {
        let entries = self.file.read_all()
            .map_err(|e| shared::SimpleDbError::CannotReadWalEntries(self.keyspace_id, self.memtable_id, e))?;
        let mut current_ptr = entries.as_slice();
        let mut entries: Vec<WalEntry> = Vec::new();
        let mut current_offset = 0;

        while current_ptr.has_remaining() {
            let start_entry_ptr = current_ptr.clone();
            let mut entry_bytes_size = 0;

            let key_len = current_ptr.get_u32_le() as usize;
            let key_timestmap = current_ptr.get_u64_le() as shared::TxnId;
            entry_bytes_size = entry_bytes_size + 12;

            let key_bytes = &current_ptr[..key_len];
            current_ptr.advance(key_len);
            entry_bytes_size = entry_bytes_size + key_len;
            let key = key::create(Bytes::from(key_bytes.to_vec()), key_timestmap);

            let value_len = current_ptr.get_u32_le() as usize;
            entry_bytes_size = entry_bytes_size + 4;
            let value_bytes = &current_ptr[..value_len];
            current_ptr.advance(value_len);
            entry_bytes_size = entry_bytes_size + value_len;

            let expected_crc = current_ptr.get_u32_le();
            let actual_crc = crc32fast::hash(&start_entry_ptr[..entry_bytes_size]);
            entry_bytes_size = entry_bytes_size + 4;

            if expected_crc != actual_crc {
                return Err(shared::SimpleDbError::CannotDecodeWal(self.keyspace_id, self.memtable_id, shared::DecodeError {
                    error_type: shared::DecodeErrorType::CorruptedCrc(actual_crc, expected_crc),
                    offset: current_offset,
                    index: entries.len(),
                }));
            }

            entries.push(WalEntry{
                value: Bytes::copy_from_slice(value_bytes.clone()),
                key
            });

            current_offset = current_offset + entry_bytes_size;
        }

        Ok(entries)
    }

    pub fn delete_wal(&mut self) -> Result<(), std::io::Error> {
        self.file.delete()
    }

    pub fn get_memtable_id(&self) -> shared::MemtableId {
        self.memtable_id
    }

    pub fn get_persisted_wal_id(
        options: &Arc<shared::SimpleDbOptions>,
        keyspace_id: shared::KeyspaceId
    ) -> Result<(Vec<Wal>, shared::MemtableId), shared::SimpleDbError> {
        let path = shared::get_directory_usize(&options.base_path, keyspace_id);
        let path = path.as_path();
        let mut max_memtable_id: usize = 0;
        let mut wals: Vec<Wal> = Vec::new();

        for file in fs::read_dir(path).map_err(|e| shared::SimpleDbError::CannotReadWalFiles(keyspace_id, e))? {
            let file = file.unwrap();

            if !Self::is_wal_file(&file) {
                continue;
            }

            if let Ok(memtable_id) = Self::extract_memtable_id_from_file(&file) {
                max_memtable_id = max(max_memtable_id, memtable_id);
                wals.push(Wal{
                    file: shared::SimpleDbFile::open(file.path().as_path(), shared::SimpleDbFileMode::AppendOnly)
                        .map_err(|e| shared::SimpleDbError::CannotReadWalFiles(keyspace_id, e))?,
                    options: options.clone(),
                    keyspace_id,
                    memtable_id,
                });
            }
        }

        Ok((wals, max_memtable_id))
    }

    fn encode(&self, key: &Key, value: &[u8]) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::new();
        //Key
        encoded.put_u32_le(key.len() as u32);
        encoded.put_u64_le(key.txn_id() as u64);
        encoded.extend(key.as_bytes());
        //Value
        encoded.put_u32_le(value.len() as u32);
        encoded.extend(value);

        encoded.put_u32_le(crc32fast::hash(&encoded));

        encoded
    }

    fn extract_memtable_id_from_file(file: &DirEntry) -> Result<shared::MemtableId, ()> {
        shared::extract_number_from_file_name(file, "-")
    }

    fn is_wal_file(file: &DirEntry) -> bool {
        file.file_name().to_str().unwrap().starts_with("wal-")
    }

    fn to_wal_file_path(
        options: &Arc<shared::SimpleDbOptions>,
        memtable_id: shared::MemtableId,
        keyspace_id: shared::KeyspaceId
    ) -> PathBuf {
        let wal_file_name = format!("wal-{}", memtable_id);
        shared::get_file_usize(&options.base_path, keyspace_id, wal_file_name.as_str())
    }
}