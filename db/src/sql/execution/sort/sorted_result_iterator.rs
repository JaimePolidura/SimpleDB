use crate::sql::execution::sort::sort_file::SortFile;
use crate::table::row::RowIterator;
use crate::table::table::Table;
use crate::Row;
use shared::SimpleDbError;
use std::sync::Arc;

#[derive(Clone)]
pub struct SortedResultIterator {
    sorted_file: SortFile,
    table: Arc<Table>,

    rows_to_return: Vec<Row>,
    current_offset: usize,
}

impl SortedResultIterator {
    pub fn create(
        sorted_file: SortFile,
        table: Arc<Table>,
    ) -> SortedResultIterator {
        SortedResultIterator {
            rows_to_return: Vec::new(),
            current_offset: 0,
            sorted_file,
            table
        }
    }
}

impl RowIterator for SortedResultIterator {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        if self.current_offset < self.sorted_file.size() && self.rows_to_return.is_empty() {
            let (row_bytes, n_rows, next_offset) = self.sorted_file.read_row_bytes(self.current_offset)?
                .unwrap();

            self.current_offset = next_offset;
            self.rows_to_return = Row::deserialize_rows(&row_bytes, n_rows, self.table.get_schema());
            return Ok(Some(self.rows_to_return.remove(0)));
        } else if !self.rows_to_return.is_empty() {
            return Ok(Some(self.rows_to_return.remove(0)));
        } else {
            return Ok(None);
        }
    }
}