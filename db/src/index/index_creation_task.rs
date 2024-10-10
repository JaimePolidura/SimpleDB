use crate::table::record::Record;
use crate::table::table::Table;
use shared::{ColumnId, KeyspaceId, SimpleDbError};
use std::sync::{mpsc, Arc};
use std::sync::mpsc::{Receiver, Sender};
use storage::transactions::transaction::Transaction;
use shared::iterators::storage_iterator::StorageIterator;
use shared::logger::logger;
use shared::logger::SimpleDbLayer::DB;
use storage::Storage;
use crate::database::database::Database;

pub struct IndexCreationTask {
    table: Arc<Table>,
    database: Arc<Database>,

    indexed_column_id: ColumnId,
    index_keyspace_id: KeyspaceId,
    table_keyspace_id: KeyspaceId,
    storage: Arc<Storage>,

    n_affected_rows_sender: Sender<Result<usize, SimpleDbError>>
}

impl IndexCreationTask {
    pub fn create(
        indexed_column_id: ColumnId,
        index_keyspace_id: KeyspaceId,
        table_keyspace_id: KeyspaceId,
        database: Arc<Database>,
        storage: Arc<Storage>,
        table: Arc<Table>,
    ) -> (IndexCreationTask, Receiver<Result<usize, SimpleDbError>>) {
        let (send, receiver) = mpsc::channel();

        let index = IndexCreationTask {
            n_affected_rows_sender: send,
            index_keyspace_id,
            table_keyspace_id,
            indexed_column_id,
            database,
            storage,
            table,
        };

        (index, receiver)
    }

    pub fn start(&self) {
        let mut n_affected_rows = 0;
        let mut iterator = self.storage.scan_all_with_transaction(
            &Transaction::none(),
            self.table_keyspace_id,
        ).unwrap();

        logger().info(DB(self.table.table_name.clone()), &format!(
            "Creating secondary index for table {} Secondary index keyspace ID: {}",
            self.table.table_name.clone(), self.index_keyspace_id
        ));

        //This will get unlocked when it goes out of scope
        let lock = self.database.lock_rollbacks();

        while iterator.next() {
            let key = iterator.key();
            let value = iterator.value();
            let mut record = Record::deserialize(value.to_vec());

            if let Some(value_to_be_indexed) = record.take_value(self.indexed_column_id) {
                n_affected_rows += 1;

                if let Err(error) = self.storage.set_with_transaction(
                    self.index_keyspace_id,
                    &Transaction::create(key.txn_id()),
                    value_to_be_indexed,
                    key.as_bytes()
                ) {
                    self.n_affected_rows_sender.send(Err(error)).unwrap();
                    return;
                }
            }
        }

        logger().info(DB(self.table.table_name.clone()), &format!(
            "Created secondary index for table {} with {} entries",
            self.table.table_name.clone(), n_affected_rows
        ));

        self.n_affected_rows_sender.send(Ok(n_affected_rows)).unwrap();
    }
}