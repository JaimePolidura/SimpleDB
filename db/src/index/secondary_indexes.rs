use crate::index::secondary_index::{SecondaryIndex, SecondaryIndexState};
use crate::table::record::Record;
use crate::table::table_descriptor::TableDescriptor;
use crate::table::table_flags::KEYSPACE_TABLE_INDEX;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use shared::{ColumnId, KeyspaceId, SimpleDbError};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use storage::Storage;

pub struct SecondaryIndexes {
    secondary_index_by_column_id: SkipMap<ColumnId, Arc<SecondaryIndex>>,
    storage: Arc<Storage>
}

impl SecondaryIndexes {
    pub fn create_empty(storage: Arc<Storage>) -> SecondaryIndexes {
        SecondaryIndexes {
            secondary_index_by_column_id: SkipMap::new(),
            storage
        }
    }

    pub fn load_secondary_indexes(
        table_descriptor: &TableDescriptor,
        storage: Arc<Storage>
    ) -> SecondaryIndexes {
        let mut secondary_indexes = SkipMap::new();
        for entry in table_descriptor.columns.iter() {
            let column_descriptor = entry.value();

            if let Some(secondary_index_keyspace_id) = column_descriptor.secondary_index_keyspace_id {
                let secondary_index = Arc::new(SecondaryIndex::create(
                    storage.clone(),
                    SecondaryIndexState::Active,
                    secondary_index_keyspace_id
                ));
                secondary_indexes.insert(column_descriptor.column_id, secondary_index);
            }
        }

        SecondaryIndexes {
            secondary_index_by_column_id: secondary_indexes,
            storage
        }
    }

    pub fn create_new_secondary_index(
        &self,
        column_id: ColumnId,
    ) -> Result<KeyspaceId, SimpleDbError> {
        let keyspace_id = self.storage.create_keyspace(KEYSPACE_TABLE_INDEX)?;

        self.secondary_index_by_column_id.insert(column_id, Arc::new(SecondaryIndex::create(
            self.storage.clone(),
            SecondaryIndexState::Creating,
            keyspace_id
        )));

        Ok(keyspace_id)
    }

    pub fn update_all(
        &self,
        transaction: &Transaction,
        primary_key: Bytes,
        new_data: &Record,
        old_data: &Record,
    ) -> Result<(), SimpleDbError> {
        for (column_id, column_value) in &new_data.data_records {
            if let Some(secondary_index_entry) = self.secondary_index_by_column_id.get(column_id) {
                let secondary_index = secondary_index_entry.value();

                secondary_index.update(
                    transaction,
                    column_value.clone(),
                    primary_key.clone(),
                    old_data.get_value(*column_id)
                )?;
            }
        }

        Ok(())
    }

    pub fn has(&self, column_id: ColumnId) -> bool {
        self.secondary_index_by_column_id.contains_key(&column_id)
    }
}