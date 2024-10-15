use crate::database::database::Database;
use crate::index::posting_list::PostingList;
use crate::table::record::Record;
use crate::table::table::Table;
use crate::Column;
use bytes::Bytes;
use shared::iterators::storage_iterator::StorageIterator;
use shared::logger::logger;
use shared::logger::SimpleDbLayer::DB;
use shared::{KeyspaceId, SimpleDbError, Value};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc};
use storage::transactions::transaction::Transaction;
use storage::Storage;

pub struct IndexCreationTask {
    table: Arc<Table>,
    database: Arc<Database>,

    index_keyspace_id: KeyspaceId,
    table_keyspace_id: KeyspaceId,
    storage: Arc<Storage>,

    secondary_indexed_column: Column,

    n_affected_rows_sender: Sender<Result<usize, SimpleDbError>>
}

impl IndexCreationTask {
    pub fn create(
        secondary_indexed_column: Column,
        index_keyspace_id: KeyspaceId,
        table_keyspace_id: KeyspaceId,
        database: Arc<Database>,
        storage: Arc<Storage>,
        table: Arc<Table>,
    ) -> (IndexCreationTask, Receiver<Result<usize, SimpleDbError>>) {
        let (send, receiver) = mpsc::channel();

        let index = IndexCreationTask {
            secondary_indexed_column,
            n_affected_rows_sender: send,
            index_keyspace_id,
            table_keyspace_id,
            database,
            storage,
            table,
        };

        (index, receiver)
    }

    pub fn start(&self) {
        let primary_column_type = &self.table.get_schema().get_primary_column().column_type;
        let secondary_column_type = &self.secondary_indexed_column.column_type;
        let mut n_affected_rows = 0;

        let mut iterator = self.storage.scan_all_with_transaction(
            &Transaction::none(),
            self.table_keyspace_id,
        ).unwrap();

        logger().info(DB(self.table.table_name.clone()), &format!(
            "Creating secondary index for table {} Secondary index keyspace ID: {} Column indexed: {}",
            self.table.table_name.clone(), self.index_keyspace_id, self.secondary_indexed_column.column_name
        ));

        //This will get unlocked when it goes out of scope
        let lock = self.database.lock_rollbacks();

        while iterator.next() {
            let primary_key = iterator.key();
            let record_bytes = iterator.value();
            let mut record = Record::deserialize(record_bytes.to_vec());

            if let Some(value_to_be_indexed) = record.take_column_bytes(self.secondary_indexed_column.column_id) {
                let posting_list = PostingList::crate_only_one_entry(primary_key);
                n_affected_rows += 1;

                logger().debug(DB(self.table.table_name.clone()), &format!(
                    "Adding secondary index entry. Table: {} Primary key: {:?} Secondary key: {:?}",
                    self.table.table_name,
                    Value::create(primary_key.as_bytes().clone(), primary_column_type.clone()).unwrap().to_string(),
                    Value::create(value_to_be_indexed.clone(), secondary_column_type.clone()).unwrap().to_string(),
                ));

                if let Err(error) = self.storage.set_with_transaction(
                    self.index_keyspace_id,
                    &Transaction::create(primary_key.txn_id()),
                    value_to_be_indexed,
                    &Bytes::from(posting_list.serialize())
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