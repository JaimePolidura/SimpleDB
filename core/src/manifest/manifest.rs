use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use bytes::{Buf, BufMut};
use serde::{Deserialize, Deserializer, Serialize};
use crate::compaction::compaction::CompactionTask;
use crate::lsm_options::LsmOptions;
use crate::utils::lsm_file::{LsmFile, LsmFileMode};

#[derive(Serialize, Deserialize)]
pub struct MemtableFlushManifestRecord {
    pub memtable_id: usize,
    pub sstable_id: usize,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    MemtableFlush(MemtableFlushManifestRecord), //Memtable id, SSTable Id
    Compaction(CompactionTask),
    None
}

pub struct Manifest {
    file: Mutex<LsmFile>,
}

impl Manifest {
    pub fn new(options: Arc<LsmOptions>) -> Result<Manifest, ()> {
        match LsmFile::open(Self::manifest_path(&options).as_path(), LsmFileMode::AppendOnly) {
            Ok(file) => Ok(Manifest{file: Mutex::new(file)}),
            Err(_) => Err(())
        }
    }

    pub fn read_records(&self) -> Result<Vec<ManifestRecord>, ()> {
        let mut file_lock_result = self.file.lock();
        let mut file = file_lock_result
            .as_mut()
            .unwrap();
        let records_bytes = file.read_all()?;
        let mut records_bytes_ptr = records_bytes.as_slice();
        let mut records: Vec<ManifestRecord> = Vec::new();

        while records_bytes_ptr.has_remaining() {
            let json_length = records_bytes_ptr.get_u32_le() as usize;
            let json_record_bytes = &records_bytes_ptr[..json_length];
            let deserialized_record = serde_json::from_slice::<ManifestRecord>(json_record_bytes)
                .map_err(|e| ())?;

            records.push(deserialized_record);
            records_bytes_ptr.advance(json_length);
        }

        file.clear();

        Ok(records)
    }

    pub fn append_record(&self, record: ManifestRecord) -> Result<(), ()> {
        let mut file_lock_result = self.file.lock();
        let file = file_lock_result
            .as_mut()
            .unwrap();

        match serde_json::to_vec(&record) {
            Ok(record_json_serialized) => {
                let mut serialized: Vec<u8> = Vec::new();
                serialized.put_u32_le(record_json_serialized.len() as u32);
                serialized.extend(record_json_serialized);
                file.write(&serialized)?;
                file.fsync();
                Ok(())
            }
            Err(_) => Err(())
        }
    }

    fn manifest_path(options: &Arc<LsmOptions>) -> PathBuf {
        let mut path_buf = PathBuf::from(&options.base_path);
        path_buf.push("MANIFEST");
        path_buf
    }
}