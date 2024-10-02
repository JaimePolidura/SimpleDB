use bytes::Bytes;
use shared::{KeyspaceId, SimpleDbError};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;

pub struct SecondaryIndex {
    keyspace_id: KeyspaceId,
    storage: Arc<storage::Storage>
}

impl SecondaryIndex {
    pub fn create(
        keyspace_id: KeyspaceId,
        storage: Arc<storage::Storage>
    ) -> SecondaryIndex {
        SecondaryIndex { keyspace_id, storage }
    }

    pub fn update(
        &self,
        transaction: &Transaction,
        column_value: Bytes, //New column value indexed
        primary_key: Bytes, //Table's primary key
        old_value: Option<&Bytes>
    ) -> Result<(), SimpleDbError> {
        todo!()
    }

    pub fn delete(
        &self,
        transaction: &Transaction,
        column_value: Bytes, //New column value indexed
        primary_key: Bytes //Table's primary key
    ) -> Result<(), SimpleDbError> {
        todo!()
    }
}