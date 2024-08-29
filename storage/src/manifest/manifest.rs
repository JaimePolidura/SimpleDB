use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use bytes::{Buf, BufMut};
use serde::{Deserialize, Deserializer, Serialize};
use crate::compaction::compaction::CompactionTask;
use crate::lsm_error::{DecodeError, DecodeErrorType, LsmError};
use crate::lsm_error::LsmError::{CannotCreateManifest, CannotDecodeManifest, CannotReadManifestOperations, CannotResetManifest};
use crate::lsm_options::LsmOptions;
use crate::utils::lsm_file::{LsmFile, LsmFileMode};
use crate::utils::utils;

pub struct Manifest {
    file: Mutex<LsmFile>,
    last_manifest_record_id: AtomicUsize,
    options: Arc<LsmOptions>
}

#[derive(Serialize, Deserialize)]
struct ManifestOperation {
    content: ManifestOperationContent,
    manifest_operation_id: usize,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum ManifestOperationContent {
    MemtableFlush(MemtableFlushManifestOperation), //Memtable id, SSTable Id
    Compaction(CompactionTask),
    Completed(usize)
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MemtableFlushManifestOperation {
    pub memtable_id: usize,
    pub sstable_id: usize,
}

impl Manifest {
    pub fn new(options: Arc<LsmOptions>) -> Result<Manifest, LsmError> {
        match LsmFile::open(Self::manifest_path(&options).as_path(), LsmFileMode::AppendOnly) {
            Ok(file) => Ok(Manifest {
                last_manifest_record_id: AtomicUsize::new(0),
                file: Mutex::new(file),
                options
            }),
            Err(e) => Err(CannotCreateManifest(e))
        }
    }

    pub fn read_uncompleted_operations(&self) -> Result<Vec<ManifestOperationContent>, LsmError> {
        let mut all_records = self.read_all_operations_from_disk()?;
        let uncompleted_operations = self.get_uncompleted_operations(&mut all_records);
        self.rewrite_manifest(&uncompleted_operations)?;

        Ok(uncompleted_operations)
    }

    fn rewrite_manifest(&self, uncompleted_operations: &Vec<ManifestOperationContent>) -> Result<(), LsmError> {
        self.clear_manifest()?;

        for uncompleted_operation in uncompleted_operations {
            self.append_operation(uncompleted_operation.clone())?;
        }

        Ok(())
    }

    fn clear_manifest(&self) -> Result<(), LsmError> {
        let path = Self::manifest_path(&self.options);
        let mut file = LsmFile::open(path.as_path(), LsmFileMode::RandomWrites)
            .map_err(|e| CannotResetManifest(e))?;

        file.clear()
            .map_err(|e| CannotResetManifest(e))
    }

    fn get_uncompleted_operations(&self, all_operations: &mut Vec<ManifestOperation>) -> Vec<ManifestOperationContent> {
        let mut operations_by_id: HashMap<usize, ManifestOperation> = HashMap::new();
        let mut to_return: Vec<ManifestOperationContent> = Vec::new();

        while let Some(operation) = utils::pop_front(all_operations) {
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

    fn read_all_operations_from_disk(&self) -> Result<Vec<ManifestOperation>, LsmError> {
        let mut file_lock_result = self.file.lock();
        let mut file = file_lock_result
            .as_mut()
            .unwrap();
        let records_bytes = file.read_all()
            .map_err(|e| CannotReadManifestOperations(e))?;
        let mut records_bytes_ptr = records_bytes.as_slice();
        let mut all_records: Vec<ManifestOperation> = Vec::new();
        let mut current_offset = 0;

        while records_bytes_ptr.has_remaining() {
            let json_length = records_bytes_ptr.get_u32_le() as usize;
            let expected_crc = records_bytes_ptr.get_u32_le();
            let json_record_bytes = &records_bytes_ptr[..json_length];
            let actual_crc = crc32fast::hash(json_record_bytes);

            if expected_crc != actual_crc {
                return Err(CannotDecodeManifest(DecodeError {
                    error_type: DecodeErrorType::CorruptedCrc(expected_crc, actual_crc),
                    index: all_records.len(),
                    offset: current_offset,
                    path: file.path(),
                }));
            }

            let deserialized_record = serde_json::from_slice::<ManifestOperation>(json_record_bytes)
                .map_err(|e| CannotDecodeManifest(DecodeError {
                    error_type: DecodeErrorType::JsonSerdeDeserialization(e),
                    index: all_records.len(),
                    offset: current_offset,
                    path: file.path(),
                }))?;

            all_records.push(deserialized_record);
            records_bytes_ptr.advance(json_length);
            current_offset = current_offset + 4 + 4 + json_length;
        }

        Ok(all_records)
    }

    pub fn mark_as_completed(&self, operation_id: usize) -> Result<usize, LsmError> {
        self.append_operation(ManifestOperationContent::Completed(operation_id))
    }

    pub fn append_operation(&self, content: ManifestOperationContent) -> Result<usize, LsmError> {
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
                serialized.put_u32_le(crc32fast::hash(&record_json_serialized));
                serialized.extend(record_json_serialized);

                file.write(&serialized)
                    .map_err(|e| LsmError::CannotWriteManifestOperation(manifest_record.content.clone(), e))?;
                file.fsync();
                Ok(manifest_record_id)
            }
            //This won't happen since manifest_record does not contain a map with non string keys
            //and Serialization implementation doesn't fail
            Err(_) => panic!("Unexpected failure of json serialization of ManifestOperationContent")
        }
    }

    fn manifest_path(options: &Arc<LsmOptions>) -> PathBuf {
        let mut path_buf = PathBuf::from(&options.base_path);
        path_buf.push("MANIFEST");
        path_buf
    }
}