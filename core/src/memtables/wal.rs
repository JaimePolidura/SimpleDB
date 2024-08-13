use std::cmp::max;
use std::fs;
use std::fs::DirEntry;
use std::io::SeekFrom::End;
use std::path::PathBuf;
use std::sync::Arc;
use bytes::{Buf, BufMut, Bytes};
use crate::key;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
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
    pub fn create(lsm_options: Arc<LsmOptions>, memtable_id: usize) -> Result<Wal, ()> {
        Ok(Wal {
            file: LsmFile::open(Self::to_wal_file_name(&lsm_options, memtable_id).as_path(), LsmFileMode::AppendOnly)?,
            lsm_options,
            memtable_id,
        })
    }

    pub fn clear_wal(&mut self) -> Result<(), ()> {
        self.file.clear()
    }

    pub fn read_entries(&self) -> Result<Vec<WalEntry>, ()> {
        let entries = self.file.read_all()?;
        let mut current_ptr = entries.as_slice();
        let mut entries: Vec<WalEntry> = Vec::new();

        while current_ptr.has_remaining() {
            let key_len = current_ptr.get_u32_le() as usize;
            let key_bytes = &current_ptr[..key_len];
            current_ptr.advance(key_len);
            let key_string = String::from_utf8(key_bytes.to_vec()).map_err(|e| ())?;
            let key = key::new(key_string.as_str());

            let value_len = current_ptr.get_u32_le() as usize;
            let value_bytes = &current_ptr[..value_len];
            current_ptr.advance(value_len);

            entries.push(WalEntry{
                value: Bytes::copy_from_slice(value_bytes),
                key,
            });
        }

        Ok(entries)
    }

    pub fn write(&mut self, key: &Key, value: &[u8]) -> Result<(), ()> {
        let encoded = self.encode(key, value);
        self.file.write(&encoded)?;
        self.file.fsync()?;
        Ok(())
    }

    pub fn get_memtable_id(&self) -> usize {
        self.memtable_id
    }

    pub fn get_persisted_wal_id(lsm_options: &Arc<LsmOptions>) -> Result<(Vec<Wal>, usize), ()> {
        let path = PathBuf::from(&lsm_options.base_path);
        let path = path.as_path();
        let mut max_memtable_id: usize = 0;
        let mut wals: Vec<Wal> = Vec::new();

        for file in fs::read_dir(path).expect("Failed to read base path") {
            let file = file.unwrap();

            if !Self::is_wal_file(&file) {
                continue;
            }

            if let Ok(memtable_id) = Self::extract_memtable_id_from_file(&file) {
                max_memtable_id = max(max_memtable_id, memtable_id);
                wals.push(Wal{
                    file: LsmFile::open(file.path().as_path(), LsmFileMode::AppendOnly)?,
                    lsm_options: lsm_options.clone(),
                    memtable_id,
                });
            }
        }

        Ok((wals, max_memtable_id))
    }
    fn encode(&self, key: &Key, value: &[u8]) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::new();
        encoded.put_u32_le(key.len() as u32);
        encoded.extend(key.as_bytes());
        encoded.put_u32_le(value.len() as u32);
        encoded.extend(value);
        encoded
    }

    fn extract_memtable_id_from_file(file: &DirEntry) -> Result<usize, ()> {
        utils::extract_number_from_file_name(file, "-")
    }

    fn is_wal_file(file: &DirEntry) -> bool {
        file.file_name().to_str().unwrap().starts_with("WAL-")
    }

    fn to_wal_file_name(lsm_options: &Arc<LsmOptions>, memtable_id: usize) -> PathBuf {
        let mut path_buff = PathBuf::from(&lsm_options.base_path);
        let wal_file_name = format!("WAL-{}", memtable_id);
        path_buff.push(wal_file_name);
        path_buff
    }
}