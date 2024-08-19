use std::cell::UnsafeCell;
use std::io::empty;
use std::path::PathBuf;
use std::sync::Arc;
use bytes::BufMut;
use log::log;
use crate::lsm_error::LsmError;
use crate::lsm_error::LsmError::{CannotCreateTransactionLog, CannotWriteTransactionLogEntry};
use crate::lsm_options::{DurabilityLevel, LsmOptions};
use crate::utils::lsm_file::{LsmFile, LsmFileMode};

const START_BINARY_CODE: u8 = 0x01;
const COMMIT_BINARY_CODE: u8 = 0x02;
const ROLLBACK_BINARY_CODE: u8 = 0x03;

pub enum TransactionLogEntry {
    START(usize), //1
    COMMIT(usize), //2
    ROLLBACK(usize) //3
}

pub struct TransactionLog {
    log_file: UnsafeCell<LsmFile>, //As log file is append only. Concurrency is resolved by OS
    lsm_options: Arc<LsmOptions>
}

impl TransactionLog {
    pub fn create(lsm_options: Arc<LsmOptions>) -> Result<TransactionLog, LsmError> {
        TransactionLog {
            log_file: UnsafeCell::new(LsmFile::open(Self::to_transaction_log_file_path(&lsm_options).as_path(),
                LsmFileMode::AppendOnly).map_err(|e| CannotCreateTransactionLog(e))?),
            lsm_options
        }
    }

    pub fn add_entry(&self, entry: TransactionLogEntry) -> Result<(), LsmError> {
        //Multiple threads can write to the WAL concurrently, since the kernel already makes sure
        //that there won't be race conditions when multiple threads are writing to an append only file
        //https://nullprogram.com/blog/2016/08/03/
        let log_file = unsafe { &mut *self.log_file.get() };

        log_file.write(self.encode(entry))
            .map_err(|e| CannotWriteTransactionLogEntry(e))?;

        if matches!(self.lsm_options.durability_level, DurabilityLevel::Strong) {
            log_file.fsync();
        }
        
        Ok(())
    }

    pub fn read_entries(&self) -> Result<Vec<TransactionLogEntry>, LsmError> {
        let mut entries: Vec<TransactionLogEntry> = Vec::new();
        let log_file = unsafe { &*self.log_file.get() };

        log_file.read_all();

        unimplemented!();
    }

    fn encode(&self, entry: TransactionLogEntry) -> Vec<u8> {
        let entry_encoded = entry.encode();
        let crc_entry = crc32fast::hash(&entry_encoded);
        let mut encoded_to_return: Vec<u8> = Vec::new();
        encoded_to_return.put_u32_le(crc_entry);
        encoded_to_return.extend(entry_encoded);

        encoded_to_return
    }

    fn to_transaction_log_file_path(lsm_options: &Arc<LsmOptions>) -> PathBuf {
        let mut path = PathBuf::from(&lsm_options.base_path);
        path.push("transaction-log");
        path
    }
}

impl TransactionLogEntry {
    pub fn encode(&self) -> Vec<u8> {
        let mut entry_encoded = Vec::new();
        match *self {
            TransactionLogEntry::ROLLBACK(txn_id) => {
                entry_encoded.put_u8(ROLLBACK_BINARY_CODE);
                entry_encoded.put_u64_le(txn_id);
            },
            TransactionLogEntry::COMMIT(txn_id) => {
                entry_encoded.put_u8(COMMIT_BINARY_CODE);
                entry_encoded.put_u64_le(txn_id);
            } ,
            TransactionLogEntry::START(txn_id) => {
                entry_encoded.put_u8(START_BINARY_CODE);
                entry_encoded.put_u64_le(txn_id);
            },
        }

        entry_encoded
    }
}