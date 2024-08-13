use std::path::PathBuf;
use std::sync::Arc;
use bytes::{BufMut, Bytes};
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::utils::lsm_file::{LsmFile, LsmFileMode};

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
            file: LsmFile::open(Self::wal_path(&lsm_options, memtable_id).as_path(), LsmFileMode::AppendOnly)?,
            lsm_options,
            memtable_id,
        })
    }

    pub fn read_entries(&self) -> Vec<WalEntry> {
        unimplemented!();
    }

    pub fn write(&mut self, key: &Key, value: &[u8]) -> Result<(), ()> {
        let encoded = self.encode(key, value);
        self.file.write(&encoded)?;
        self.file.fsync()?;
        Ok(())
    }

    fn encode(&self, key: &Key, value: &[u8]) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::new();
        encoded.put_u32_le(key.len() as u32);
        encoded.extend(key.as_bytes());
        encoded.put_u32_le(value.len() as u32);
        encoded.extend(value);
        encoded
    }

    fn wal_path(lsm_options: &Arc<LsmOptions>, memtable_id: usize) -> PathBuf {
        let mut path_buff = PathBuf::from(&lsm_options.base_path);
        let wal_file_name = format!("WAL-{}", memtable_id);
        path_buff.push(wal_file_name);
        path_buff
    }
}