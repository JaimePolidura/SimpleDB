use bytes::{Buf, BufMut};
use std::cell::UnsafeCell;
use std::path::PathBuf;
use std::sync::Arc;

const ROLLED_BACK_ACTIVE_TRANSACTION_FAILURE_BINARY_CODE: u8 = 0x05;
const ROLLEDBACK_WRITE_BINARY_CODE: u8 = 0x04;
const START_ROLLBACK_BINARY_CODE: u8 = 0x03;
const COMMIT_BINARY_CODE: u8 = 0x02;
const START_BINARY_CODE: u8 = 0x01;

pub enum TransactionLogEntry {
    //Transaction has been started
    Start(shared::TxnId),
    //Transaction has been commited
    Commit(shared::TxnId),
    //Rollback has started. Writes done by the transaction will be discarded at memtable flush & SSTable compaction
    //usize is the number of writes that the transaction has done
    StartRollback(shared::TxnId, usize),
    //A write done by a rolledback transaction (marked in the log with StartRollback) has been discarded
    RolledbackWrite(shared::TxnId),
    //When the Lsm boots, if it finds an active transaction (not commited or rolledback) and there is no present write
    //in the lsm engine with that transaction ID. It will be marked with RolledbackActiveTransactionFailure
    //When the LSM reboots again the transaction will be ignored
    RolledbackActiveTransactionFailure(shared::TxnId)
}

pub struct TransactionLog {
    //Wrapped with RwLock Becase TransactionLog needs to be passed to threads. UnsafeCell doest implement Sync
    log_file: shared::SimpleDbFileWrapper,
    options: Arc<shared::SimpleDbOptions>
}

impl TransactionLog {
    pub fn create(options: Arc<shared::SimpleDbOptions>) -> Result<TransactionLog, shared::SimpleDbError> {
        Ok(TransactionLog {
            log_file: shared::SimpleDbFileWrapper {file: UnsafeCell::new(
                shared::SimpleDbFile::open(to_transaction_log_file_path(&options).as_path(),
                                           shared::SimpleDbFileMode::AppendOnly).map_err(|e| shared::SimpleDbError::CannotCreateTransactionLog(e))?) },
            options
        })
    }

    pub fn create_mock(options: Arc<shared::SimpleDbOptions>) -> TransactionLog {
        TransactionLog {
            log_file: shared::SimpleDbFileWrapper {file: UnsafeCell::new(shared::SimpleDbFile::mock())},
            options
        }
    }

    pub fn add_entry(&self, entry: TransactionLogEntry) -> Result<(), shared::SimpleDbError> {
        //Multiple threads can write to the WAL concurrently, since the kernel already makes sure
        //that there won't be race conditions when multiple threads are writing to an append only file
        //https://nullprogram.com/blog/2016/08/03/
        let log_file = unsafe { &mut *self.log_file.file.get() };

        log_file.write(&entry.encode())
            .map_err(|e| shared::SimpleDbError::CannotWriteTransactionLogEntry(e))?;

        if matches!(self.options.durability_level, shared::DurabilityLevel::Strong) {
            log_file.fsync();
        }
        
        Ok(())
    }

    pub fn replace_entries(&self, new_active_txn_id: &Vec<TransactionLogEntry>) -> Result<(), shared::SimpleDbError> {
        let log_file = unsafe { &mut *self.log_file.file.get() };
        let new_entries_encoded: Vec<u8> = new_active_txn_id.iter()
            .map(|entry| entry.encode())
            .flatten()
            .collect();

        log_file.save_write(&new_entries_encoded)
            .map_err(|e| shared::SimpleDbError::CannotResetTransactionLog(e));

        Ok(())
    }

    pub fn read_entries(&self) -> Result<Vec<TransactionLogEntry>, shared::SimpleDbError> {
        let mut entries: Vec<TransactionLogEntry> = Vec::new();
        let log_file = unsafe { &*self.log_file.file.get() };
        let entries_bytes = log_file.read_all()
            .map_err(|e| shared::SimpleDbError::CannotReadTransactionLogEntries(e))?;
        let mut current_ptr = entries_bytes.as_slice();
        let mut current_offset = 0;

        while current_ptr.has_remaining() {
            let (decoded_entry, decoded_size) = TransactionLogEntry::decode(
                &mut current_ptr,
                current_offset,
                entries.len())?;

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
    ) -> Result<(TransactionLogEntry, usize), shared::SimpleDbError> {
        let expected_crc = current_ptr.get_u32_le();
        let encoded_size = Self::encoded_size(current_ptr[0])
            .map_err(|_| shared::SimpleDbError::CannotDecodeTransactionLogEntry(shared::DecodeError {
                offset: current_offset,
                index: n_entry_to_decode,
                error_type: shared::DecodeErrorType::UnknownFlag(current_ptr[0] as usize)
            }))?;
        let actual_crc = crc32fast::hash(&current_ptr[0..encoded_size]);

        if actual_crc != expected_crc {
            return Err(shared::SimpleDbError::CannotDecodeTransactionLogEntry(shared::DecodeError {
                offset: current_offset,
                index: n_entry_to_decode,
                error_type: shared::DecodeErrorType::CorruptedCrc(expected_crc, actual_crc)
            }));
        }

        let binary_code = current_ptr.get_u8();
        let txn_id = current_ptr.get_u64_le() as shared::TxnId;

        let decoded_entry = match binary_code {
            ROLLED_BACK_ACTIVE_TRANSACTION_FAILURE_BINARY_CODE => TransactionLogEntry::RolledbackActiveTransactionFailure(txn_id),
            ROLLEDBACK_WRITE_BINARY_CODE => TransactionLogEntry::RolledbackWrite(txn_id),
            START_ROLLBACK_BINARY_CODE => {
                let n_writes = current_ptr.get_u64_le();
                TransactionLogEntry::StartRollback(txn_id, n_writes as usize)
            },
            COMMIT_BINARY_CODE => TransactionLogEntry::Commit(txn_id),
            START_BINARY_CODE => TransactionLogEntry::Start(txn_id),
            _ => return Err(shared::SimpleDbError::CannotDecodeTransactionLogEntry(shared::DecodeError {
                offset: current_offset,
                index: n_entry_to_decode,
                error_type: shared::DecodeErrorType::UnknownFlag(current_ptr[0] as usize)
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
        entry_encoded.put_u8(self.serialize());
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

    pub fn txn_id(&self) -> shared::TxnId {
        match *self {
            TransactionLogEntry::RolledbackActiveTransactionFailure(txn_id) => txn_id,
            TransactionLogEntry::StartRollback(txn_id, _) => txn_id,
            TransactionLogEntry::Commit(txn_id) => txn_id,
            TransactionLogEntry::Start(txn_id) => txn_id,
            TransactionLogEntry::RolledbackWrite(txn_id) => txn_id,
        }
    }

    pub fn serialize(&self) -> u8 {
        match *self {
            TransactionLogEntry::RolledbackActiveTransactionFailure(_) => ROLLED_BACK_ACTIVE_TRANSACTION_FAILURE_BINARY_CODE,
            TransactionLogEntry::RolledbackWrite(_) => ROLLEDBACK_WRITE_BINARY_CODE,
            TransactionLogEntry::StartRollback(_, _) => START_ROLLBACK_BINARY_CODE,
            TransactionLogEntry::Commit(_txn_id) => COMMIT_BINARY_CODE,
            TransactionLogEntry::Start(_txn_id) => START_BINARY_CODE,
        }
    }
}

fn to_transaction_log_file_path(options: &Arc<shared::SimpleDbOptions>) -> PathBuf {
    let mut path = PathBuf::from(&options.base_path);
    path.push("transaction-log");
    path
}