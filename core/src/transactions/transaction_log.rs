use std::cell::UnsafeCell;
use std::path::PathBuf;
use std::sync::Arc;
use bytes::{Buf, BufMut};
use crate::lsm_error::{DecodeError, DecodeErrorType, LsmError};
use crate::lsm_error::LsmError::{CannotCreateTransactionLog, CannotDecodeTransactionLogEntry, CannotReadTransactionLogEntries, CannotResetTransacionLog, CannotWriteTransactionLogEntry};
use crate::lsm_options::{DurabilityLevel, LsmOptions};
use crate::transactions::transaction::TxnId;
use crate::utils::lsm_file::{LsmFile, LsmFileMode};

const START_BINARY_CODE: u8 = 0x01;
const COMMIT_BINARY_CODE: u8 = 0x02;
const ROLLBACK_BINARY_CODE: u8 = 0x03;

pub enum TransactionLogEntry {
    START(TxnId), //1
    COMMIT(TxnId), //2
    ROLLBACK(TxnId) //3
}

pub struct TransactionLog {
    log_file: UnsafeCell<LsmFile>, //As log file is append only. Concurrency is resolved by OS
    lsm_options: Arc<LsmOptions>
}

impl TransactionLog {
    pub fn create(lsm_options: Arc<LsmOptions>) -> Result<TransactionLog, LsmError> {
        Ok(TransactionLog {
            log_file: UnsafeCell::new(LsmFile::open(Self::to_transaction_log_file_path(&lsm_options).as_path(),
                LsmFileMode::AppendOnly).map_err(|e| CannotCreateTransactionLog(e))?),
            lsm_options
        })
    }

    pub fn add_entry(&self, entry: TransactionLogEntry) -> Result<(), LsmError> {
        //Multiple threads can write to the WAL concurrently, since the kernel already makes sure
        //that there won't be race conditions when multiple threads are writing to an append only file
        //https://nullprogram.com/blog/2016/08/03/
        let log_file = unsafe { &mut *self.log_file.get() };

        log_file.write(&self.encode(entry))
            .map_err(|e| CannotWriteTransactionLogEntry(e))?;

        if matches!(self.lsm_options.durability_level, DurabilityLevel::Strong) {
            log_file.fsync();
        }
        
        Ok(())
    }

    pub fn replace_entries(&self, new_active_txn_id: &Vec<TxnId>) -> Result<(), LsmError> {
        let log_file = unsafe { &mut *self.log_file.get() };
        let new_entries_encoded: Vec<u8> = new_active_txn_id.iter()
            .map(|txn_id| TransactionLogEntry::START(txn_id))
            .map(|entry| entry.encode())
            .flatten()
            .collect();

        //TODO Unsafe operation. If failure occurs in these instructions, we will lose data
        log_file.clear().map_err(|e| CannotResetTransacionLog(e))?;
        log_file.write(new_entries_encoded).map_err(|e| CannotResetTransacionLog(e))?;

        Ok(())
    }

    pub fn read_entries(&self) -> Result<Vec<TransactionLogEntry>, LsmError> {
        let mut entries: Vec<TransactionLogEntry> = Vec::new();
        let log_file = unsafe { &*self.log_file.get() };
        let entries_bytes = log_file.read_all()
            .map_err(|e| CannotReadTransactionLogEntries(e))?;
        let mut current_ptr = entries_bytes.as_slice();
        let mut current_offset = 0;

        while current_ptr.has_remaining() {
            let entry_start_offset = current_offset;
            let expected_crc = current_ptr.get_u32_le();
            let binary_code = current_ptr.get_u8();
            let txn_id = current_ptr.get_u64_le() as TxnId;
            current_offset = current_offset + 13;

            let actual_crc = crc32fast::hash(&entries_bytes[entry_start_offset+4..entry_start_offset+13]);

            if actual_crc != expected_crc {
                return Err(CannotDecodeTransactionLogEntry(DecodeError{
                    path: Self::to_transaction_log_file_path(&self.lsm_options),
                    offset: current_offset,
                    index: entries.len(),
                    error_type: DecodeErrorType::CorruptedCrc(expected_crc, actual_crc)
                }));
            }

            match binary_code {
                ROLLBACK_BINARY_CODE => entries.push(TransactionLogEntry::ROLLBACK(txn_id)),
                COMMIT_BINARY_CODE => entries.push(TransactionLogEntry::COMMIT(txn_id)),
                START_BINARY_CODE => entries.push(TransactionLogEntry::START(txn_id)),
                _ => return Err(CannotDecodeTransactionLogEntry(DecodeError {
                    path: Self::to_transaction_log_file_path(&self.lsm_options),
                    offset: current_offset,
                    index: entries.len(),
                    error_type: DecodeErrorType::UnknownFlag(binary_code as usize)
                }))
            };
        }

        Ok(entries)
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
                entry_encoded.put_u64_le(txn_id as u64);
            },
            TransactionLogEntry::COMMIT(txn_id) => {
                entry_encoded.put_u8(COMMIT_BINARY_CODE);
                entry_encoded.put_u64_le(txn_id as u64);
            } ,
            TransactionLogEntry::START(txn_id) => {
                entry_encoded.put_u8(START_BINARY_CODE);
                entry_encoded.put_u64_le(txn_id as u64);
            },
        }

        entry_encoded
    }

    pub fn txn_id(&self) -> TxnId {
        match *self {
            TransactionLogEntry::ROLLBACK(txn_id) => txn_id,
            TransactionLogEntry::COMMIT(txn_id) => txn_id,
            TransactionLogEntry::START(txn_id) => txn_id
        }
    }
}