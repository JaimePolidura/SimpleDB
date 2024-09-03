use crate::keyspace::keyspace::{Keyspace, KeyspaceId};
use crate::lsm_error::LsmError;
use crate::lsm_error::LsmError::KeyspaceNotFound;
use crate::lsm_options::LsmOptions;
use crate::memtables::memtable::MemtableIterator;
use crate::sst::ssttable_iterator::SSTableIterator;
use crate::transactions::transaction::{Transaction, TxnId};
use crate::transactions::transaction_manager::{IsolationLevel, TransactionManager};
use crate::utils::merge_iterator::MergeIterator;
use crate::utils::two_merge_iterators::TwoMergeIterator;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub struct Lsm {
    transaction_manager: Arc<TransactionManager>,
    keyspaces: SkipMap<KeyspaceId, Arc<Keyspace>>,
    lsm_options: Arc<LsmOptions>,
}

pub enum WriteBatch {
    Put(KeyspaceId, String, Bytes),
    Delete(KeyspaceId, String)
}

pub type LsmIterator = TwoMergeIterator<MergeIterator<MemtableIterator>, MergeIterator<SSTableIterator>>;

pub fn new(lsm_options: Arc<LsmOptions>) -> Result<Lsm, LsmError> {
    println!("Starting mini lsm engine!");
    let transaction_manager = Arc::new(
        TransactionManager::create_recover_from_log(lsm_options.clone())?
    );
    let keyspaces = create_keyspaces(
        &transaction_manager, &lsm_options
    )?;

    let mut lsm = Lsm {
        transaction_manager,
        keyspaces,
        lsm_options
    };

    lsm.rollback_active_transactions();
    lsm.recover_from_manifest();
    lsm.start_keyspaces_compaction_threads();

    println!("Mini lsm engine started!");

    Ok(lsm)
}

fn create_keyspaces(
    transaction_manager: &Arc<TransactionManager>,
    lsm_options: &Arc<LsmOptions>
) -> Result<SkipMap<KeyspaceId, Arc<Keyspace>>, LsmError> {
    unimplemented!();
}

impl Lsm {
    pub fn scan_all(&self, keyspace_id: KeyspaceId) -> Result<LsmIterator, LsmError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        self.scan_all_with_transaction(keyspace_id, &transaction)
    }

    pub fn scan_all_with_transaction(
        &self,
        keyspace_id: KeyspaceId,
        transaction: &Transaction
    ) -> Result<LsmIterator, LsmError> {
        let keyspace = self.get_keyspace(keyspace_id)?;
        Ok(keyspace.scan_all_with_transaction(transaction))
    }

    pub fn get(
        &self,
        keyspace_id: KeyspaceId,
        key: &str
    ) -> Result<Option<bytes::Bytes>, LsmError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        self.get_with_transaction(keyspace_id, &transaction, key)
    }

    pub fn get_with_transaction(
        &self,
        keyspace_id: KeyspaceId,
        transaction: &Transaction,
        key: &str,
    ) -> Result<Option<bytes::Bytes>, LsmError> {
        let keyspace = self.get_keyspace(keyspace_id)?;
        keyspace.get_with_transaction(transaction, key)
    }

    pub fn set(
        &self,
        keyspace_id: KeyspaceId,
        key: &str,
        value: &[u8]
    ) -> Result<(), LsmError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        self.set_with_transaction(keyspace_id, &transaction, key, value)
    }

    pub fn set_with_transaction(
        &self,
        keyspace_id: KeyspaceId,
        transaction: &Transaction,
        key: &str,
        value: &[u8],
    ) -> Result<(), LsmError> {
        let keyspace = self.get_keyspace(keyspace_id)?;
        keyspace.set_with_transaction(transaction, key, value)
    }

    pub fn delete(
        &self,
        keyspace_id: KeyspaceId,
        key: &str
    ) -> Result<(), LsmError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::ReadUncommited);
        self.delete_with_transaction(keyspace_id, &transaction, key)
    }

    pub fn delete_with_transaction(
        &self,
        keyspace_id: KeyspaceId,
        transaction: &Transaction,
        key: &str,
    ) -> Result<(), LsmError> {
        let keyspace = self.get_keyspace(keyspace_id)?;
        keyspace.delete_with_transaction(transaction, key)
    }

    pub fn write_batch(&self, batch: &[WriteBatch]) -> Result<(), LsmError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        for write_batch_record in batch {
            match write_batch_record {
                WriteBatch::Put(keyspace_id, key, value) => {
                    self.set_with_transaction(*keyspace_id, &transaction, key.as_str(), value)?
                },
                WriteBatch::Delete(keyspace_id, key) => {
                    self.delete_with_transaction(*keyspace_id, &transaction, key.as_str())?
                }
            };
        }

        Ok(())
    }

    pub fn start_transaction_with_isolation(&self, isolation_level: IsolationLevel) -> Transaction {
        self.transaction_manager.start_transaction(isolation_level)
    }

    pub fn start_transaction(&self) -> Transaction {
        self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation)
    }

    pub fn commit_transaction(&self, transaction: Transaction) {
        self.transaction_manager.commit(transaction);
    }

    pub fn rollback_transaction(&self, transaction: Transaction) {
        self.transaction_manager.rollback(transaction);
    }

    fn get_keyspace(&self, keyspace_id: KeyspaceId) -> Result<Arc<Keyspace>, LsmError> {
        match self.keyspaces.get(&keyspace_id) {
            Some(entry) => Ok(entry.value().clone()),
            None => Err(KeyspaceNotFound(keyspace_id))
        }
    }

    //The following functions are used when booting up the minilsm storage engine
    fn rollback_active_transactions(&mut self) {
        let active_transactions_id = self.transaction_manager.get_active_transactions();

        for active_transaction_id in active_transactions_id {
            if self.has_txn_id_been_written(active_transaction_id) {
                self.transaction_manager.rollback(Transaction {
                    active_transactions: HashSet::new(),
                    isolation_level: IsolationLevel::SnapshotIsolation,
                    n_writes_rolled_back: AtomicUsize::new(0),
                    n_writes: AtomicUsize::new(usize::MAX),
                    txn_id: active_transaction_id
                });
            } else {
                self.transaction_manager.rollback_active_transaction_failure(active_transaction_id);
            }
        }
    }

    fn has_txn_id_been_written(&self, txn_id: TxnId) -> bool {
        for keyspace in self.keyspaces.iter() {
            let keyspace = keyspace.value();
            if keyspace.has_txn_id_been_written(txn_id) {
                return true;
            }
        }

        false
    }

    fn start_keyspaces_compaction_threads(&self) {
        for keyspace in self.keyspaces.iter() {
            let keyspace = keyspace.value();
            keyspace.start_compaction_thread();
        }
    }

    fn recover_from_manifest(&mut self) {
        for keyspace in self.keyspaces.iter() {
            let keyspace = keyspace.value();
            keyspace.recover_from_manifest();
        }
    }
}