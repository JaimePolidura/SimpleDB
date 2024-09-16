use crate::table::record::{Record, RecordBuilder};
use crate::table::row::Row;
use bytes::Bytes;
use shared::ColumnId;
use std::sync::Arc;
use storage::utils::storage_iterator::StorageIterator;
use storage::SimpleDbStorageIterator;
use crate::table::table::Table;

struct RowReconstruction {
    is_fully_reconstructed: bool,
    record_builder: RecordBuilder,
    key: Bytes,

    selection: Arc<Vec<ColumnId>>
}

pub struct TableIterator {
    simple_db_storage_iterator: SimpleDbStorageIterator,
    selection: Arc<Vec<ColumnId>>, //Columns ID to retrieve from storage engine
    rows_in_reconstruction: Vec<RowReconstruction>,
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
            rows_in_reconstruction: Vec::new(),
            simple_db_storage_iterator,
            current_row: None,
            table,
        }
    }

    pub fn next(&mut self) -> bool {
        while self.n_reconstructed_rows_that_can_be_returned() > 0 {
            if !self.simple_db_storage_iterator.next() {
                break;
            }

            let record = Record::deserialize(self.simple_db_storage_iterator.value().to_vec());
            let key = Bytes::copy_from_slice(self.simple_db_storage_iterator.key().as_bytes());
            self.reconstruct_row(key, record);
        }

        if self.rows_in_reconstruction.is_empty() {
            return false;
        }

        let row_in_reconstruction = self.rows_in_reconstruction.pop().unwrap();
        let key_bytes = row_in_reconstruction.key.clone();
        let row_record_reconstructed = row_in_reconstruction.build();
        self.current_row = Some(Row::create(row_record_reconstructed, &self.selection, &self.table, key_bytes));

        true
    }

    pub fn row(&self) -> &Row {
        self.current_row.as_ref().unwrap()
    }

    fn reconstruct_row(&mut self, key: Bytes, record: Record) {
        let row_reconstruction_index = match self.find_row_reconstruction_index(&key) {
            Some(row_reconstruction_index) => row_reconstruction_index,
            None => {
                self.rows_in_reconstruction.push(RowReconstruction{
                    selection: self.selection.clone(),
                    record_builder: Record::builder(),
                    is_fully_reconstructed: false,
                    key,
                });
                self.rows_in_reconstruction.len() - 1
            }
        };

        let mut row_reconstruction = &mut self.rows_in_reconstruction[row_reconstruction_index];
        row_reconstruction.add_record(record);
    }

    fn find_row_reconstruction_index(&self, key: &Bytes) -> Option<usize> {
        for (current_index, current_row_reconstruction) in self.rows_in_reconstruction.iter().enumerate() {
            if current_row_reconstruction.key.eq(key) {
                return Some(current_index);
            }
        }

        None
    }

    fn n_reconstructed_rows_that_can_be_returned(&self) -> usize {
        let mut n_reconstructed_rows = 0;
        for row_in_reconstruction in &self.rows_in_reconstruction {
            if row_in_reconstruction.is_fully_reconstructed {
                n_reconstructed_rows = n_reconstructed_rows + 1;
            } else {
                break
            }
        }

        n_reconstructed_rows
    }
}

impl RowReconstruction {
    pub fn add_record(&mut self, record: Record) {
        self.record_builder.add_record(record);
        if self.record_builder.has_columns_id(self.selection.as_ref()) {
            self.is_fully_reconstructed = true;
        }
    }

    pub fn build(self) -> Record {
        self.record_builder.build()
    }
}