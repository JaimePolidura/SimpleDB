use std::cmp::max;
use std::fs;
use std::fs::DirEntry;
use std::path::PathBuf;
use std::sync::Arc;
use bytes::{Buf, BufMut, Bytes};
use crate::key;
use crate::key::Key;
use crate::lsm_error::{DecodeError, DecodeErrorType, LsmError};
use crate::lsm_error::LsmError::{CannotCreateWal, CannotDecodeWal, CannotReadWalEntries, CannotReadWalFiles, CannotWriteWalEntry};
use crate::lsm_options::{DurabilityLevel, LsmOptions};
use crate::utils::lsm_file::{LsmFile, LsmFileMode};
use crate::utils::utils;

pub struct Wal {
    lsm_options: Arc<LsmOptions>,
    memtable_id: usize,
    file: LsmFile
}

pub(crate) struct WalEntry {
    pub key: Key,
    pub value: Bytes
}

impl Wal {
    pub fn create(lsm_options: Arc<LsmOptions>, memtable_id: usize) -> Result<Wal, LsmError> {
        Ok(Wal {
            file: LsmFile::open(Self::to_wal_file_name(&lsm_options, memtable_id).as_path(), LsmFileMode::AppendOnly)
                .map_err(|e| CannotCreateWal(memtable_id, e))?,
            lsm_options,
            memtable_id,
        })
    }

    pub fn create_mock(lsm_options: Arc<LsmOptions>, memtable_id: usize) -> Result<Wal, LsmError> {
        Ok(Wal {
            file: LsmFile::mock(),
            lsm_options,
            memtable_id,
        })
    }

    pub fn write(&mut self, key: &Key, value: &[u8]) -> Result<(), LsmError> {
        let encoded = self.encode(key, value);
        self.file.write(&encoded)
            .map_err(|e| CannotWriteWalEntry(self.memtable_id, e));

        if matches!(self.lsm_options.durability_level, DurabilityLevel::Strong) {
            self.file.fsync();
        }

        Ok(())
    }

    pub fn read_entries(&self) -> Result<Vec<WalEntry>, LsmError> {
        let entries = self.file.read_all()
            .map_err(|e| CannotReadWalEntries(self.memtable_id, e))?;
        let mut current_ptr = entries.as_slice();
        let mut entries: Vec<WalEntry> = Vec::new();
        let mut current_offset = 0;

        while current_ptr.has_remaining() {
            let start_entry_ptr = current_ptr.clone();
            let mut entry_bytes_size = 0;

            let key_len = current_ptr.get_u32_le() as usize;
            let key_timestmap = current_ptr.get_u64_le();
            entry_bytes_size = entry_bytes_size + 12;

            let key_bytes = &current_ptr[..key_len];
            current_ptr.advance(key_len);
            entry_bytes_size = entry_bytes_size + key_len;
            let key_string = String::from_utf8(key_bytes.to_vec())
                .map_err(|e| CannotDecodeWal(self.memtable_id, DecodeError {
                    path: self.file.path(),
                    offset: current_offset,
                    index: entries.len(),
                    error_type: DecodeErrorType::Utf8Decode(e)
                }))?;

            let key = key::new(key_string.as_str(), key_timestmap);

            let value_len = current_ptr.get_u32_le() as usize;
            entry_bytes_size = entry_bytes_size + 4;
            let value_bytes = &current_ptr[..value_len];
            current_ptr.advance(value_len);
            entry_bytes_size = entry_bytes_size + value_len;

            let expected_crc = current_ptr.get_u32_le();
            let actual_crc = crc32fast::hash(&start_entry_ptr[..entry_bytes_size]);
            entry_bytes_size = entry_bytes_size + 4;

            if expected_crc != actual_crc {
                return Err(CannotDecodeWal(self.memtable_id, DecodeError {
                    path: self.file.path(),
                    offset: current_offset,
                    index: entries.len(),
                    error_type: DecodeErrorType::CorruptedCrc(actual_crc, expected_crc),
                }));
            }

            entries.push(WalEntry{
                value: Bytes::copy_from_slice(value_bytes),
                key,
            });

            current_offset = current_offset + entry_bytes_size;
        }

        Ok(entries)
    }

    pub fn delete_wal(&mut self) -> Result<(), std::io::Error> {
        self.file.delete()
    }

    pub fn get_memtable_id(&self) -> usize {
        self.memtable_id
    }

    pub fn get_persisted_wal_id(lsm_options: &Arc<LsmOptions>) -> Result<(Vec<Wal>, usize), LsmError> {
        let path = PathBuf::from(&lsm_options.base_path);
        let path = path.as_path();
        let mut max_memtable_id: usize = 0;
        let mut wals: Vec<Wal> = Vec::new();

        for file in fs::read_dir(path).map_err(|e| CannotReadWalFiles(e))? {
            let file = file.unwrap();

            if !Self::is_wal_file(&file) {
                continue;
            }

            if let Ok(memtable_id) = Self::extract_memtable_id_from_file(&file) {
                max_memtable_id = max(max_memtable_id, memtable_id);
                wals.push(Wal{
                    file: LsmFile::open(file.path().as_path(), LsmFileMode::AppendOnly)
                        .map_err(|e| CannotReadWalFiles(e))?,
                    lsm_options: lsm_options.clone(),
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
        encoded.put_u64_le(key.txn_id());
        encoded.extend(key.as_bytes());
        //Value
        encoded.put_u32_le(value.len() as u32);
        encoded.extend(value);

        encoded.put_u32_le(crc32fast::hash(&encoded));

        encoded
    }

    fn extract_memtable_id_from_file(file: &DirEntry) -> Result<usize, ()> {
        utils::extract_number_from_file_name(file, "-")
    }

    fn is_wal_file(file: &DirEntry) -> bool {
        file.file_name().to_str().unwrap().starts_with("wal-")
    }

    fn to_wal_file_name(lsm_options: &Arc<LsmOptions>, memtable_id: usize) -> PathBuf {
        let mut path_buff = PathBuf::from(&lsm_options.base_path);
        let wal_file_name = format!("wal-{}", memtable_id);
        path_buff.push(wal_file_name);
        path_buff
    }
}