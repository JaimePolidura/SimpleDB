use crate::lsm_error::LsmError::{CannotCreateTransactionLog, CannotDecodeTransactionLogEntry, CannotReadTransactionLogEntries, CannotResetTransacionLog, CannotWriteTransactionLogEntry};
use crate::lsm_error::{DecodeError, DecodeErrorType, LsmError};
use crate::lsm_options::{DurabilityLevel, LsmOptions};
use crate::transactions::transaction::TxnId;
use crate::utils::lsm_file::{LsmFile, LsmFileMode};
use bytes::{Buf, BufMut};
use std::cell::UnsafeCell;
use std::path::PathBuf;
use std::sync::Arc;

const ROLLEDBACK_WRITE_BINARY_CODE: u8 = 0x04;
const START_ROLLBACK_BINARY_CODE: u8 = 0x03;
const COMMIT_BINARY_CODE: u8 = 0x02;
const START_BINARY_CODE: u8 = 0x01;

pub enum TransactionLogEntry {
    Start(TxnId),
    Commit(TxnId),
    StartRollback(TxnId, usize), //Nº Total writes
    RolledbackWrite(TxnId),
}

//UnsafeCell does not implement Sync, so it cannot be passed to threads
//We need to wrap it in a struct that implements Sync
pub struct LsmFileWrapper {
    file: UnsafeCell<LsmFile>,
}

unsafe impl Send for LsmFileWrapper {}
unsafe impl Sync for LsmFileWrapper {}

pub struct TransactionLog {
    //As log file is append only. Concurrency is resolved by OS
    //Wrapped with RwLock Becase TransactionLog needs to be passed to threads. UnsafeCell doest implement Sync
    log_file: LsmFileWrapper,
    lsm_options: Arc<LsmOptions>
}

impl TransactionLog {
    pub fn create(lsm_options: Arc<LsmOptions>) -> Result<TransactionLog, LsmError> {
        Ok(TransactionLog {
            log_file: LsmFileWrapper {file: UnsafeCell::new(LsmFile::open(to_transaction_log_file_path(&lsm_options).as_path(),
                                                                          LsmFileMode::AppendOnly).map_err(|e| CannotCreateTransactionLog(e))?) },
            lsm_options
        })
    }

    pub fn create_mock(lsm_options: Arc<LsmOptions>) -> TransactionLog {
        TransactionLog {
            log_file: LsmFileWrapper {file: UnsafeCell::new(LsmFile::mock())},
            lsm_options
        }
    }

    pub fn add_entry(&self, entry: TransactionLogEntry) -> Result<(), LsmError> {
        //Multiple threads can write to the WAL concurrently, since the kernel already makes sure
        //that there won't be race conditions when multiple threads are writing to an append only file
        //https://nullprogram.com/blog/2016/08/03/
        let log_file = unsafe { &mut *self.log_file.file.get() };

        log_file.write(&entry.encode())
            .map_err(|e| CannotWriteTransactionLogEntry(e))?;

        if matches!(self.lsm_options.durability_level, DurabilityLevel::Strong) {
            log_file.fsync();
        }
        
        Ok(())
    }

    pub fn replace_entries(&self, new_active_txn_id: &Vec<TransactionLogEntry>) -> Result<(), LsmError> {
        let log_file = unsafe { &mut *self.log_file.file.get() };
        let new_entries_encoded: Vec<u8> = new_active_txn_id.iter()
            .map(|entry| entry.encode())
            .flatten()
            .collect();

        //TODO Unsafe operation. If failure occurs in these instructions, we will lose data
        log_file.clear().map_err(|e| CannotResetTransacionLog(e))?;
        log_file.write(&new_entries_encoded).map_err(|e| CannotResetTransacionLog(e))?;

        Ok(())
    }

    pub fn read_entries(&self) -> Result<Vec<TransactionLogEntry>, LsmError> {
        let mut entries: Vec<TransactionLogEntry> = Vec::new();
        let log_file = unsafe { &*self.log_file.file.get() };
        let entries_bytes = log_file.read_all()
            .map_err(|e| CannotReadTransactionLogEntries(e))?;
        let mut current_ptr = entries_bytes.as_slice();
        let mut current_offset = 0;

        while current_ptr.has_remaining() {
            let (decoded_entry, decoded_size) = TransactionLogEntry::decode(
                &mut current_ptr,
                current_offset,
                entries.len(),
                &self.lsm_options
            )?;

            current_offset = current_offset + decoded_size;
            entries.push(decoded_entry);
        }

        Ok(entries)
    }
}

impl TransactionLogEntry {
    pub fn decode(
        current_ptr: &mut &[u8],
        current_offset: usize,
        n_entry_to_decode: usize,
        lsm_options: &Arc<LsmOptions>,
    ) -> Result<(TransactionLogEntry, usize), LsmError> {
        let expected_crc = current_ptr.get_u32_le();
        let encoded_size = Self::encoded_size(current_ptr[0])
            .map_err(|_| CannotDecodeTransactionLogEntry(DecodeError {
                path: to_transaction_log_file_path(lsm_options),
                offset: current_offset,
                index: n_entry_to_decode,
                error_type: DecodeErrorType::UnknownFlag(current_ptr[0] as usize)
            }))?;
        let actual_crc = crc32fast::hash(&current_ptr[0..encoded_size]);

        if actual_crc != expected_crc {
            return Err(CannotDecodeTransactionLogEntry(DecodeError{
                path: to_transaction_log_file_path(lsm_options),
                offset: current_offset,
                index: n_entry_to_decode,
                error_type: DecodeErrorType::CorruptedCrc(expected_crc, actual_crc)
            }));
        }

        let binary_code = current_ptr.get_u8();
        let txn_id = current_ptr.get_u64_le() as TxnId;

        let decoded_entry = match binary_code {
            ROLLEDBACK_WRITE_BINARY_CODE => TransactionLogEntry::RolledbackWrite(txn_id),
            START_ROLLBACK_BINARY_CODE => {
                let n_writes = current_ptr.get_u64_le();
                TransactionLogEntry::StartRollback(txn_id, n_writes as usize)
            },
            COMMIT_BINARY_CODE => TransactionLogEntry::Commit(txn_id),
            START_BINARY_CODE => TransactionLogEntry::Start(txn_id),
            _ => return Err(CannotDecodeTransactionLogEntry(DecodeError {
                path: to_transaction_log_file_path(lsm_options),
                offset: current_offset,
                index: n_entry_to_decode,
                error_type: DecodeErrorType::UnknownFlag(current_ptr[0] as usize)
            }))
        };

        Ok((decoded_entry, encoded_size))
    }

    pub fn encoded_size(binary_code: u8) -> Result<usize, ()> {
        match binary_code {
            ROLLEDBACK_WRITE_BINARY_CODE => Ok(1 + 8),
            START_ROLLBACK_BINARY_CODE => Ok(1 + 8 + 8),
            COMMIT_BINARY_CODE => Ok(1 + 8),
            START_BINARY_CODE => Ok(1 + 8),
            _ => Err(())
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut entry_encoded = Vec::new();
        entry_encoded.put_u8(self.get_binary_code());
        match *self {
            TransactionLogEntry::StartRollback(txn_id, n_writes) => {
                entry_encoded.put_u64_le(txn_id as u64);
                entry_encoded.put_u64_le(n_writes as u64);
            },
            _ => entry_encoded.put_u64_le(self.txn_id() as u64),
        }

        let crc_entry = crc32fast::hash(&entry_encoded);
        let mut encoded_to_return: Vec<u8> = Vec::new();
        encoded_to_return.put_u32_le(crc_entry);
        encoded_to_return.extend(entry_encoded);

        encoded_to_return
    }

    pub fn txn_id(&self) -> TxnId {
        match *self {
            TransactionLogEntry::StartRollback(txn_id, _) => txn_id,
            TransactionLogEntry::Commit(txn_id) => txn_id,
            TransactionLogEntry::Start(txn_id) => txn_id,
            TransactionLogEntry::RolledbackWrite(txn_id) => txn_id
        }
    }

    pub fn get_binary_code(&self) -> u8 {
        match *self {
            TransactionLogEntry::RolledbackWrite(_) => ROLLEDBACK_WRITE_BINARY_CODE,
            TransactionLogEntry::StartRollback(_, _) => START_ROLLBACK_BINARY_CODE,
            TransactionLogEntry::Commit(_txn_id) => COMMIT_BINARY_CODE,
            TransactionLogEntry::Start(_txn_id) => START_BINARY_CODE,
        }
    }
}

fn to_transaction_log_file_path(lsm_options: &Arc<LsmOptions>) -> PathBuf {
    let mut path = PathBuf::from(&lsm_options.base_path);
    path.push("transaction-log");
    path
}