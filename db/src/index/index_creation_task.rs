use crate::table::record::Record;
use crate::table::table::Table;
use shared::{ColumnId, KeyspaceId};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use shared::iterators::storage_iterator::StorageIterator;
use storage::Storage;
use crate::database::database::Database;

pub struct IndexCreationTask {
    table: Arc<Table>,
    database: Arc<Database>,

    indexed_column_id: ColumnId,
    index_keyspace_id: KeyspaceId,
    storage: Arc<Storage>
}

impl IndexCreationTask {
    pub fn create(
        indexed_column_id: ColumnId,
        keyspace_id: KeyspaceId,
        database: Arc<Database>,
        storage: Arc<Storage>,
        table: Arc<Table>,
    ) -> IndexCreationTask {
        IndexCreationTask { table, indexed_column_id, index_keyspace_id: keyspace_id, storage, database }
    }

    pub fn start(&self) -> usize {
        let mut n_affected_rows = 0;
        let mut iterator = self.storage.scan_all_with_transaction(
            &Transaction::none(),
            self.index_keyspace_id,
        ).unwrap();
        
        //This will get unlocked when it goes out of scope
        let guard = self.database.lock_rollbacks();

        while iterator.next() {
            let key = iterator.key();
            let value = iterator.value();
            let mut record = Record::deserialize(value.to_vec());

            if let Some(value_to_be_indexed) = record.take_value(self.indexed_column_id) {
                n_affected_rows += 1;

                self.storage.set_with_transaction(
                    self.index_keyspace_id,
                    &Transaction::create(key.txn_id()),
                    value_to_be_indexed,
                    key.as_bytes()
                );
            }
        }

        n_affected_rows
    }
}