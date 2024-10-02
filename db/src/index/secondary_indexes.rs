use crate::index::secondary_index::SecondaryIndex;
use crate::table::record::Record;
use crate::table::table_descriptor::TableDescriptor;
use crossbeam_skiplist::SkipMap;
use shared::{ColumnId, SimpleDbError};
use std::sync::Arc;
use bytes::Bytes;
use storage::transactions::transaction::Transaction;
use storage::Storage;

pub struct SecondaryIndexes {
    secondary_index_by_column_id: SkipMap<ColumnId, Arc<SecondaryIndex>>
}

impl SecondaryIndexes {
    pub fn create_empty() -> SecondaryIndexes {
        SecondaryIndexes {
            secondary_index_by_column_id: SkipMap::new(),
        }
    }

    pub fn create_from_table_descriptor(
        table_descriptor: &TableDescriptor,
        storage: Arc<Storage>
    ) -> SecondaryIndexes {
        let mut secondary_indexes = SkipMap::new();
        for entry in table_descriptor.columns.iter() {
            let column_descriptor = entry.value();

            if let Some(secondary_index_keyspace_id) = column_descriptor.secondary_index_keyspace_id {
                let secondary_index = Arc::new(SecondaryIndex::create(secondary_index_keyspace_id, storage.clone()));
                secondary_indexes.insert(column_descriptor.column_id, secondary_index);
            }
        }

        SecondaryIndexes {
            secondary_index_by_column_id: secondary_indexes
        }
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
}