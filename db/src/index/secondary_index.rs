use crate::index::posting_list::PostingList;
use crate::index::secondary_index_iterator::SecondaryIndexIterator;
use bytes::Bytes;
use shared::logger::logger;
use shared::logger::SimpleDbLayer::DB;
use shared::{KeyspaceId, SimpleDbError, SimpleDbOptions, Type};
use std::sync::Arc;
use shared::key::Key;
use storage::transactions::transaction::Transaction;
use storage::{SimpleDbStorageIterator, Storage};

pub enum SecondaryIndexState {
    Creating,
    Active
}

pub struct SecondaryIndex {
    keyspace_id: KeyspaceId,
    storage: Arc<storage::Storage>,
    state: SecondaryIndexState,
    table_name: String,
    primary_column_type: Type,
}

impl SecondaryIndex {
    pub fn create(
        storage: Arc<storage::Storage>,
        state: SecondaryIndexState,
        keyspace_id: KeyspaceId,
        table_name: String,
        primary_column_type: Type,
    ) -> SecondaryIndex {
        SecondaryIndex { keyspace_id, storage, state, table_name, primary_column_type }
    }

    pub fn create_mock() -> SecondaryIndex {
        SecondaryIndex {
            storage: Arc::new(Storage::create_mock(&Arc::new(SimpleDbOptions::default()))),
            table_name: String::from("table_name"),
            state: SecondaryIndexState::Active,
            primary_column_type: Type::I64,
            keyspace_id: 1,
        }
    }

    pub fn update(
        &self,
        transaction: &Transaction,
        new_value: Bytes, //New column value indexed
        primary_key: Bytes, //Table's primary key
        old_value: Option<&Bytes>
    ) -> Result<(), SimpleDbError> {
        logger().info(DB(self.table_name.clone()), &format!(
            "Updating secondary index. ID: {:?} New value: {:?} Old value {:?}",
            primary_key, new_value, old_value
        ));

        if let Some(old_value) = old_value {
            self.delete(transaction, old_value.clone(), primary_key.clone())?;
        }

        let new_entry = PostingList::crate_only_one_entry(&Key::create(primary_key, self.primary_column_type, transaction.id()))
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
        Ok(SecondaryIndexIterator::create(transaction, iterator, self.primary_column_type))
    }

    pub fn delete(
        &self,
        transaction: &Transaction,
        column_value: Bytes, //New column value indexed
        primary_key: Bytes //Table's primary key
    ) -> Result<(), SimpleDbError> {
        let deleted_entry = PostingList::create_deleted(primary_key, self.primary_column_type, transaction)
            .serialize();

        self.storage.set_with_transaction(
            self.keyspace_id,
            transaction,
            column_value,
            &deleted_entry
        )
    }

    pub fn can_be_read(&self) -> bool {
        matches!(self.state, SecondaryIndexState::Active)
    }
}