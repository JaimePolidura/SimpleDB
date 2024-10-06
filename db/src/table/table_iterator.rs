use crate::table::record::{Record, RecordBuilder};
use crate::table::row::Row;
use bytes::Bytes;
use shared::ColumnId;
use std::sync::Arc;
use shared::iterators::storage_iterator::StorageIterator;
use storage::SimpleDbStorageIterator;
use crate::table::table::Table;

//This is the iterator that will be exposed to users of the SimpleDB
//As data is stored in LSM engine, which is "only" append only, update records of rows are stored as ID -> [updated column id, new column value]
//So if we want to get all the columns of a row existing row by its ID, we will need to reassemble it because column values will
//scatter over sstables and memtables.
struct RowReassemble {
    is_fully_reassembled: bool,
    record_builder: RecordBuilder,
    key: Bytes,

    selection: Arc<Vec<ColumnId>>
}

pub struct TableIterator {
    simple_db_storage_iterator: SimpleDbStorageIterator,
    selection: Arc<Vec<ColumnId>>, //Columns ID to retrieve from storage engine
    rows_reassembling: Vec<RowReassemble>,
    current_row: Option<Row>,

    table: Arc<Table>
}

impl TableIterator {
    pub(crate) fn create(
        simple_db_storage_iterator: SimpleDbStorageIterator,
        selection: Vec<ColumnId>, //Columns ID to select
        table: Arc<Table>
    ) -> TableIterator {
        TableIterator {
            selection: Arc::new(selection),
            rows_reassembling: Vec::new(),
            simple_db_storage_iterator,
            current_row: None,
            table,
        }
    }

    pub fn next(&mut self) -> bool {
        while self.n_reassembled_rows_that_can_be_returned() == 0 {
            if !self.simple_db_storage_iterator.next() {
                break;
            }

            let record = Record::deserialize(self.simple_db_storage_iterator.value().to_vec());
            let key = Bytes::copy_from_slice(self.simple_db_storage_iterator.key().as_bytes());
            self.reassemble_row(key, record);
        }

        if self.rows_reassembling.is_empty() {
            return false;
        }

        let row_in_reassembling = self.rows_reassembling.remove(0);
        let key_bytes = row_in_reassembling.key.clone();
        let row_record_reassembled = row_in_reassembling.build();
        self.current_row = Some(Row::create(row_record_reassembled, &self.selection, &self.table, key_bytes));

        true
    }

    pub fn row(&self) -> &Row {
        self.current_row.as_ref().unwrap()
    }

    fn reassemble_row(&mut self, key: Bytes, record: Record) {
        let row_reassemble_index = match self.find_row_reassemble_index(&key) {
            Some(row_reassemble_index) => row_reassemble_index,
            None => {
                let mut record_builder = Record::builder();
                record_builder.add_column(self.table.get_primary_column_data().unwrap().column_id, key.clone());

                self.rows_reassembling.push(RowReassemble {
                    selection: self.selection.clone(),
                    is_fully_reassembled: false,
                    record_builder,
                    key,
                });
                self.rows_reassembling.len() - 1
            }
        };

        let mut row_reassemble = &mut self.rows_reassembling[row_reassemble_index];
        row_reassemble.add_record(record);
    }
    
    fn find_row_reassemble_index(&self, key: &Bytes) -> Option<usize> {
        for (current_index, current_row_reassemble) in self.rows_reassembling.iter().enumerate() {
            if current_row_reassemble.key.eq(key) {
                return Some(current_index);
            }
        }

        None
    }

    fn n_reassembled_rows_that_can_be_returned(&self) -> usize {
        let mut n_reassembled_rows = 0;
        for row_reassembling in &self.rows_reassembling {
            if row_reassembling.is_fully_reassembled {
                n_reassembled_rows = n_reassembled_rows + 1;
            } else {
                break
            }
        }

        n_reassembled_rows
    }
}

impl RowReassemble {
    pub fn add_record(&mut self, record: Record) {
        self.record_builder.add_record(record);
        if self.record_builder.has_columns_id(self.selection.as_ref()) {
            self.is_fully_reassembled = true;
        }
    }

    pub fn build(self) -> Record {
        self.record_builder.build()
    }
}