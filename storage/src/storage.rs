use crate::keyspace::keyspaces::Keyspaces;
use crate::memtables::memtable_iterator::MemtableIterator;
use crate::sst::ssttable_iterator::SSTableIterator;
use crate::transactions::transaction::Transaction;
use crate::transactions::transaction_manager::{IsolationLevel, TransactionManager};
use shared::iterators::merge_iterator::MergeIterator;
use crate::utils::storage_engine_iterator::StorageEngineIterator;
use shared::iterators::two_merge_iterators::TwoMergeIterator;
use bytes::Bytes;
use shared::{Flag, KeyspaceId, SimpleDbError, SimpleDbOptions, Type};
use std::collections::VecDeque;
use std::sync::Arc;
use shared::logger::{logger, SimpleDbLayer};
use crate::temp::temporary_space::TemporarySpace;
use crate::temp::temporary_spaces::TemporarySpaces;

pub struct Storage {
    transaction_manager: Arc<TransactionManager>,
    temporary_spaces: TemporarySpaces,
    keyspaces: Keyspaces,
}

//Key value
pub enum WriteBatch {
    Put(KeyspaceId, Bytes, Bytes),
    Delete(KeyspaceId, Bytes)
}

pub type SimpleDbStorageIterator = StorageEngineIterator<
    TwoMergeIterator<MergeIterator<MemtableIterator>, MergeIterator<SSTableIterator>>
>;

impl Storage {
    pub fn create(options: Arc<SimpleDbOptions>) -> Result<Storage, SimpleDbError> {
        logger().info(SimpleDbLayer::Storage, "Starting storage engine!");

        let transaction_manager = Arc::new(
            TransactionManager::create_recover_from_log(options.clone())?
        );
        let keyspaces = Keyspaces::load_keyspaces(
            transaction_manager.clone(), options.clone()
        )?;

        let mut storage = Storage {
            temporary_spaces: TemporarySpaces::create(options.clone())?,
            transaction_manager,
            keyspaces,
        };

        storage.keyspaces.recover_from_manifest();
        storage.keyspaces.start_keyspaces_compaction_threads();

        logger().info(SimpleDbLayer::Storage, "Storage engine started!");

        Ok(storage)
    }

    pub fn create_mock(simple_db_options: &Arc<SimpleDbOptions>) -> Storage {
        Storage {
            transaction_manager: Arc::new(TransactionManager::create_mock(simple_db_options.clone())),
            keyspaces: Keyspaces::mock(simple_db_options.clone()),
            temporary_spaces: TemporarySpaces::create_mock(),
        }
    }

    pub fn scan_all(&self, keyspace_id: KeyspaceId) -> Result<SimpleDbStorageIterator, SimpleDbError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        let mut iterator = self.scan_all_with_transaction(&transaction, keyspace_id)?;
        iterator.set_transaction_standalone(&self.transaction_manager, transaction);
        Ok(iterator)
    }

    pub fn scan_from(
        &self,
        keyspace_id: KeyspaceId,
        key: &Bytes,
        inclusive: bool
    ) -> Result<SimpleDbStorageIterator, SimpleDbError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        let mut iterator = self.scan_from_key_with_transaction(&transaction, keyspace_id, key, inclusive)?;
        iterator.set_transaction_standalone(&self.transaction_manager, transaction);
        Ok(iterator)
    }

    pub fn scan_from_key_with_transaction(
        &self,
        transaction: &Transaction,
        keyspace_id: KeyspaceId,
        key: &Bytes,
        inclusive: bool,
    ) -> Result<SimpleDbStorageIterator, SimpleDbError> {
        let keyspace = self.keyspaces.get_keyspace(keyspace_id)?;
        Ok(keyspace.scan_from_key_with_transaction(transaction, key, inclusive))
    }

    pub fn scan_all_with_transaction(
        &self,
        transaction: &Transaction,
        keyspace_id: KeyspaceId,
    ) -> Result<SimpleDbStorageIterator, SimpleDbError> {
        let keyspace = self.keyspaces.get_keyspace(keyspace_id)?;
        Ok(keyspace.scan_all_with_transaction(transaction))
    }

    pub fn get(
        &self,
        keyspace_id: KeyspaceId,
        key: &Bytes
    ) -> Result<Option<Bytes>, SimpleDbError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        self.get_with_transaction(keyspace_id, &transaction, key)
    }

    pub fn get_with_transaction(
        &self,
        keyspace_id: KeyspaceId,
        transaction: &Transaction,
        key: &Bytes,
    ) -> Result<Option<Bytes>, SimpleDbError> {
        let keyspace = self.keyspaces.get_keyspace(keyspace_id)?;
        keyspace.get_with_transaction(transaction, key)
    }

    pub fn set(
        &self,
        keyspace_id: KeyspaceId,
        key: Bytes,
        value: &[u8]
    ) -> Result<(), SimpleDbError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        self.set_with_transaction(keyspace_id, &transaction, key, value)
    }

    pub fn set_with_transaction(
        &self,
        keyspace_id: KeyspaceId,
        transaction: &Transaction,
        key: Bytes,
        value: &[u8],
    ) -> Result<(), SimpleDbError> {
        let keyspace = self.keyspaces.get_keyspace(keyspace_id)?;
        keyspace.set_with_transaction(transaction, key, value)
    }

    pub fn delete(
        &self,
        keyspace_id: KeyspaceId,
        key: Bytes
    ) -> Result<(), SimpleDbError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::ReadUncommited);
        self.delete_with_transaction(keyspace_id, &transaction, key)
    }

    pub fn delete_with_transaction(
        &self,
        keyspace_id: KeyspaceId,
        transaction: &Transaction,
        key: Bytes,
    ) -> Result<(), SimpleDbError> {
        let keyspace = self.keyspaces.get_keyspace(keyspace_id)?;
        keyspace.delete_with_transaction(transaction, key)
    }

    pub fn write_batch(&self, batch: Vec<WriteBatch>) -> Result<(), SimpleDbError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        let mut batch = VecDeque::from(batch);

        while let Some(write_batch_record) = batch.pop_front() {
            match write_batch_record {
                WriteBatch::Put(keyspace_id, key, value) => {
                    self.set_with_transaction(keyspace_id, &transaction, key, value.as_ref())?
                },
                WriteBatch::Delete(keyspace_id, key) => {
                    self.delete_with_transaction(keyspace_id, &transaction, key)?
                }
            };
        }

        Ok(())
    }

    pub fn get_flags(&self, keyspace_id: KeyspaceId) -> Result<Flag, SimpleDbError> {
        let keyspace = self.keyspaces.get_keyspace(keyspace_id)?;
        Ok(keyspace.flags())
    }

    pub fn start_transaction_with_isolation(&self, isolation_level: IsolationLevel) -> Transaction {
        self.transaction_manager.start_transaction(isolation_level)
    }

    pub fn start_transaction(&self) -> Transaction {
        self.transaction_manager.start_transaction(IsolationLevel::SnapshotIsolation)
    }

    pub fn commit_transaction(&self, transaction: &Transaction) -> Result<(), SimpleDbError> {
        self.transaction_manager.commit(transaction)
    }

    pub fn rollback_transaction(&self, transaction: &Transaction) -> Result<(), SimpleDbError> {
        self.transaction_manager.rollback(transaction)
    }

    pub fn create_keyspace(&self, flag: Flag, key_type: Type) -> Result<KeyspaceId, SimpleDbError> {
        let keyspace = self.keyspaces.create_keyspace(flag, key_type)?;
        keyspace.start_compaction_thread();
        Ok(keyspace.keyspace_id())
    }

    pub fn create_temporary_space(&self) -> Result<TemporarySpace, SimpleDbError> {
        self.temporary_spaces.create_temporary_space()
    }

    pub fn get_keyspaces_id(&self) -> Vec<KeyspaceId> {
        self.keyspaces.get_keyspaces_id()
    }
}