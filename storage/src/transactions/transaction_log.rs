use bytes::{Buf, BufMut};
use shared::{SimpleDbError, SimpleDbFile, TxnId};
use std::cell::UnsafeCell;
use std::path::PathBuf;
use std::sync::Arc;

const START_BINARY_CODE: u8 = 0x01;
const COMMIT_BINARY_CODE: u8 = 0x02;
const WRITE_BINARY_CODE: u8 = 0x03;
const START_ROLLBACK_BINARY_CODE: u8 = 0x04;
const ROLLEDBACK_WRITE_BINARY_CODE: u8 = 0x05;
const MAX_TXNID_BINARY_CODE: u8 = 0x06;

#[derive(Clone, Debug, PartialOrd, PartialEq)]
pub enum TransactionLogEntry {
    Start(TxnId),
    Write(TxnId),
    Commit(TxnId),

    //Transaction has been rolledback
    StartRollback(TxnId),
    RolledbackWrite(TxnId),

    //After we read the transaction log, we write the max txn id found. So in this way
    //We can conserve the max txnId,
    MaxTxnId(TxnId),
}

pub struct TransactionLog {
    //Wrapped with RwLock Because TransactionLog needs to be passed to threads. UnsafeCell doest implement Sync
    log_file: shared::SimpleDbFileWrapper,
    options: Arc<shared::SimpleDbOptions>
}

impl TransactionLog {
    pub fn create(options: Arc<shared::SimpleDbOptions>) -> Result<TransactionLog, SimpleDbError> {
        Ok(TransactionLog {
            log_file: shared::SimpleDbFileWrapper {file: UnsafeCell::new(
                SimpleDbFile::open(to_transaction_log_file_path(&options).as_path(), shared::SimpleDbFileMode::AppendOnly)
                    .map_err(|e| SimpleDbError::CannotCreateTransactionLog(e))?) },
            options
        })
    }

    pub fn create_mock(options: Arc<shared::SimpleDbOptions>) -> TransactionLog {
        TransactionLog {
            log_file: shared::SimpleDbFileWrapper {file: UnsafeCell::new(shared::SimpleDbFile::create_mock())},
            options
        }
    }

    pub fn add_entry(&self, entry: TransactionLogEntry) -> Result<(), shared::SimpleDbError> {
        //Multiple threads can write to the WAL concurrently, since the kernel already makes sure
        //that there won't be race conditions when multiple threads are writing to an append only file
        //https://nullprogram.com/blog/2016/08/03/
        let log_file = unsafe { &mut *self.log_file.file.get() };

        log_file.write(&entry.serialize())
            .map_err(|e| shared::SimpleDbError::CannotWriteTransactionLogEntry(e))?;

        if matches!(self.options.durability_level, shared::DurabilityLevel::Strong) {
            let _ = log_file.fsync();
        }
        
        Ok(())
    }

    pub fn replace_entries(&self, new_active_txn_id: &Vec<TransactionLogEntry>) -> Result<(), SimpleDbError> {
        let log_file = unsafe { &mut *self.log_file.file.get() };
        let new_entries_encoded: Vec<u8> = new_active_txn_id.iter()
            .map(|entry| entry.serialize())
            .flatten()
            .collect();

        log_file.safe_replace(&new_entries_encoded)
            .map_err(|e| SimpleDbError::CannotResetTransactionLog(e))?;

        Ok(())
    }

    pub fn read_entries(&self) -> Result<Vec<TransactionLogEntry>, SimpleDbError> {
        let mut entries: Vec<TransactionLogEntry> = Vec::new();
        let log_file = unsafe { &*self.log_file.file.get() };
        let entries_bytes = log_file.read_all()
            .map_err(|e| shared::SimpleDbError::CannotReadTransactionLogEntries(e))?;
        let mut current_ptr = entries_bytes.as_slice();

        while current_ptr.has_remaining() {
            let decoded_entry = TransactionLogEntry::deserialize(
                &mut current_ptr,
                entries.len()
            )?;

            entries.push(decoded_entry);
        }

        Ok(entries)
    }
}

#[allow(suspicious_double_ref_op)]
impl TransactionLogEntry {
    pub fn deserialize(
        current_ptr: &mut &[u8],
        n_entry_index: usize
    ) -> Result<TransactionLogEntry, SimpleDbError> {
        let start_entry_ptr = current_ptr.clone();

        let entry = match current_ptr.get_u8() {
            START_BINARY_CODE => TransactionLogEntry::Start(current_ptr.get_u64_le() as TxnId),
            COMMIT_BINARY_CODE => TransactionLogEntry::Commit(current_ptr.get_u64_le() as TxnId),
            WRITE_BINARY_CODE => TransactionLogEntry::Write(current_ptr.get_u64_le() as TxnId),
            START_ROLLBACK_BINARY_CODE => TransactionLogEntry::StartRollback(current_ptr.get_u64_le() as TxnId),
            ROLLEDBACK_WRITE_BINARY_CODE => TransactionLogEntry::RolledbackWrite(current_ptr.get_u64_le() as TxnId),
            MAX_TXNID_BINARY_CODE => TransactionLogEntry::MaxTxnId(current_ptr.get_u64_le() as TxnId),
            unknown_flag => return Err(SimpleDbError::CannotDecodeTransactionLogEntry(shared::DecodeError {
                offset: n_entry_index,
                index: n_entry_index,
                error_type: shared::DecodeErrorType::UnknownFlag(unknown_flag as usize)
            }))
        };

        let expected_crc = current_ptr.get_u32_le();
        //1 -> entry type byte, 8 transaction id
        let actual_crc = crc32fast::hash(&start_entry_ptr[..(1 + 8)]);

        if expected_crc != actual_crc {
            return Err(SimpleDbError::CannotDecodeTransactionLogEntry(shared::DecodeError {
                offset: n_entry_index,
                index: n_entry_index,
                error_type: shared::DecodeErrorType::CorruptedCrc(expected_crc, actual_crc)
            }));
        }

        Ok(entry)
    }

    pub fn txn_id(&self) -> shared::TxnId {
        match &self {
            TransactionLogEntry::RolledbackWrite(id) => *id,
            TransactionLogEntry::StartRollback(id) => *id,
            TransactionLogEntry::MaxTxnId(id) => *id,
            TransactionLogEntry::Commit(id) => *id,
            TransactionLogEntry::Start(id) => *id,
            TransactionLogEntry::Write(id) => *id,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.put_u8(self.serialize_transaction_log_entry_type());
        serialized.put_u64_le(self.txn_id() as u64);
        serialized.put_u32_le(crc32fast::hash(serialized.as_slice()));
        serialized
    }

    fn serialize_transaction_log_entry_type(&self) -> u8 {
        match &self {
            TransactionLogEntry::RolledbackWrite(_) => ROLLEDBACK_WRITE_BINARY_CODE,
            TransactionLogEntry::StartRollback(_) => START_ROLLBACK_BINARY_CODE,
            TransactionLogEntry::MaxTxnId(_) => MAX_TXNID_BINARY_CODE,
            TransactionLogEntry::Commit(_) => COMMIT_BINARY_CODE,
            TransactionLogEntry::Start(_) => START_BINARY_CODE,
            TransactionLogEntry::Write(_) => WRITE_BINARY_CODE,
        }
    }
}

fn to_transaction_log_file_path(options: &Arc<shared::SimpleDbOptions>) -> PathBuf {
    let mut path = PathBuf::from(&options.base_path);
    path.push("transaction-log");
    path
}

#[cfg(test)]
mod test {
    use crate::transactions::transaction_log::TransactionLogEntry;

    #[test]
    fn entry_serialize_deserialize() {
        let entry = TransactionLogEntry::RolledbackWrite(1);
        let serialized = entry.serialize();
        let deserialized = TransactionLogEntry::deserialize(
            &mut serialized.as_slice(), 0
        ).unwrap();

        assert_eq!(deserialized, TransactionLogEntry::RolledbackWrite(1));
    }
}