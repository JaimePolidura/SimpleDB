use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use bytes::{Buf, BufMut};
use serde::{Deserialize, Deserializer, Serialize};
use crate::compaction::compaction::CompactionTask;
use crate::lsm_options::LsmOptions;
use crate::utils::lsm_file::{LsmFile, LsmFileMode};

pub struct Manifest {
    file: Mutex<LsmFile>,
    last_manifest_record_id: AtomicUsize,
}

#[derive(Serialize, Deserialize)]
struct ManifestOperation {
    content: ManifestOperationContent,
    manifest_operation_id: usize,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestOperationContent {
    MemtableFlush(MemtableFlushManifestOperation), //Memtable id, SSTable Id
    Compaction(CompactionTask),
    Completed(usize)
}

#[derive(Serialize, Deserialize)]
pub struct MemtableFlushManifestOperation {
    pub memtable_id: usize,
    pub sstable_id: usize,
}

impl Manifest {
    pub fn new(options: Arc<LsmOptions>) -> Result<Manifest, ()> {
        match LsmFile::open(Self::manifest_path(&options).as_path(), LsmFileMode::AppendOnly) {
            Ok(file) => Ok(Manifest {
                last_manifest_record_id: AtomicUsize::new(0),
                file: Mutex::new(file),
            }),
            Err(_) => Err(())
        }
    }

    pub fn read_uncompleted_operations(&self) -> Result<Vec<ManifestOperationContent>, ()> {
        let mut all_records = self.read_all_operations_from_disk()?;
        let uncompleted_operations = self.get_uncompleted_operations(&mut all_records);

        Ok(uncompleted_operations)
    }

    fn get_uncompleted_operations(&self, all_operations: &mut Vec<ManifestOperation>) -> Vec<ManifestOperationContent> {
        let mut operations_by_id: HashMap<usize, ManifestOperation> = HashMap::new();
        let mut to_return: Vec<ManifestOperationContent> = Vec::new();

        while let Some(operation) = all_operations.pop() {
            match operation.content {
                ManifestOperationContent::Completed(operation_id) => operations_by_id.remove(&operation_id),
                _ => operations_by_id.insert(operation.manifest_operation_id, operation),
            };
        }

        let operations_id_uncompleted: Vec<usize> = operations_by_id.keys()
            .into_iter()
            .map(|key| *key)
            .collect();

        for operation_id in operations_id_uncompleted {
            let operation = operations_by_id.remove(&operation_id)
                .unwrap();
            to_return.push(operation.content);
        }

        to_return
    }

    fn read_all_operations_from_disk(&self) -> Result<Vec<ManifestOperation>, ()> {
        let mut file_lock_result = self.file.lock();
        let mut file = file_lock_result
            .as_mut()
            .unwrap();
        let records_bytes = file.read_all()?;
        let mut records_bytes_ptr = records_bytes.as_slice();
        let mut all_records: Vec<ManifestOperation> = Vec::new();

        while records_bytes_ptr.has_remaining() {
            let json_length = records_bytes_ptr.get_u32_le() as usize;
            let json_record_bytes = &records_bytes_ptr[..json_length];
            let deserialized_record = serde_json::from_slice::<ManifestOperation>(json_record_bytes)
                .map_err(|e| ())?;

            all_records.push(deserialized_record);
            records_bytes_ptr.advance(json_length);
        }

        file.clear();

        Ok(all_records)
    }

    pub fn mark_as_completed(&self, operation_id: usize) {
        self.append_operation(ManifestOperationContent::Completed(operation_id));
    }

    pub fn append_operation(&self, content: ManifestOperationContent) -> Result<usize, ()> {
        let manifest_record_id = self.last_manifest_record_id.fetch_add(1, Relaxed);
        let mut file_lock_result = self.file.lock();
        let file = file_lock_result
            .as_mut()
            .unwrap();
        let manifest_record = ManifestOperation { manifest_operation_id: manifest_record_id, content, };

        match serde_json::to_vec(&manifest_record) {
            Ok(record_json_serialized) => {
                let mut serialized: Vec<u8> = Vec::new();
                serialized.put_u32_le(record_json_serialized.len() as u32);
                serialized.extend(record_json_serialized);
                file.write(&serialized)?;
                file.fsync();
                Ok(manifest_record_id)
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