use crate::index::posting_list::PostingList;
use bytes::Bytes;
use shared::{KeyspaceId, SimpleDbError};
use std::sync::Arc;
use storage::SimpleDbStorageIterator;
use storage::transactions::transaction::Transaction;
use storage::utils::storage_engine_iterator::StorageEngineIterator;
use crate::index::secondary_index_iterator::SecondaryIndexIterator;

pub enum SecondaryIndexState {
    Creating,
    Active
}

pub struct SecondaryIndex {
    keyspace_id: KeyspaceId,
    storage: Arc<storage::Storage>,
    state: SecondaryIndexState
}

impl SecondaryIndex {
    pub fn create(
        storage: Arc<storage::Storage>,
        state: SecondaryIndexState,
        keyspace_id: KeyspaceId,
    ) -> SecondaryIndex {
        SecondaryIndex { keyspace_id, storage, state }
    }

    pub fn update(
        &self,
        transaction: &Transaction,
        new_value: Bytes, //New column value indexed
        primary_key: Bytes, //Table's primary key
        old_value: Option<&Bytes>
    ) -> Result<(), SimpleDbError> {
        if let Some(old_value) = old_value {
            self.delete(transaction, old_value.clone(), primary_key.clone())?;
        }

        let new_entry = PostingList::create_deleted(primary_key, transaction)
            .serialize();

        self.storage.set_with_transaction(
            self.keyspace_id,
            transaction,
            new_value,
            &new_entry
        )
    }

    pub fn scan_all(
        &self,
        transaction: &Transaction
    ) -> Result<SecondaryIndexIterator<SimpleDbStorageIterator>, SimpleDbError> {
        let iterator = self.storage.scan_all_with_transaction(transaction, self.keyspace_id)?;
        Ok(SecondaryIndexIterator::create(transaction, iterator))
    }

    pub fn delete(
        &self,
        transaction: &Transaction,
        column_value: Bytes, //New column value indexed
        primary_key: Bytes //Table's primary key
    ) -> Result<(), SimpleDbError> {
        let deleted_entry = PostingList::create_deleted(primary_key, transaction)
            .serialize();

        self.storage.set_with_transaction(
            self.keyspace_id,
            transaction,
            column_value,
            &deleted_entry
        )
    }
}