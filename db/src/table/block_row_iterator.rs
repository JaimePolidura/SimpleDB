use crate::table::row::{MockRowIterator, RowIterator};
use crate::Row;
use shared::SimpleDbError;

//Iterates a query iterator by returning block of rows whose size is less or equal than block_size_bytes
pub struct RowBlockIterator<I: RowIterator> {
    inner_iterator: I,
    block_size_bytes: usize,
    last_row_overflows_prev_block: Option<Row>,
}

pub enum RowBlock {
    Rows(Vec<Row>), //Rows whose serialized size is less or equal than block size
    Overflow(Row) //This row overflows block size
}

impl<I: RowIterator> RowBlockIterator<I> {
    pub fn create_from_vec(
        block_size_bytes: usize,
        rows: Vec<Row>
    ) -> RowBlockIterator<MockRowIterator> {
        RowBlockIterator {
            inner_iterator: MockRowIterator::create(rows),
            last_row_overflows_prev_block: None,
            block_size_bytes,
        }
    }

    pub fn create(
        block_size_bytes: usize,
        inner_iterator: I,
    ) -> RowBlockIterator<I> {
        RowBlockIterator {
            last_row_overflows_prev_block: None,
            block_size_bytes,
            inner_iterator,
        }
    }

    pub fn next_block(&mut self) -> Result<RowBlock, SimpleDbError> {
        let mut row_size_bytes_to_return = 0;
        let mut rows_to_return = Vec::new();

        while let Some(current_row) = self.next_row()? {
            let current_row_size_bytes = current_row.serialized_size();

            if current_row_size_bytes > self.block_size_bytes && rows_to_return.is_empty() {
                return Ok(RowBlock::Overflow(current_row));
            }
            if current_row_size_bytes > self.block_size_bytes && !rows_to_return.is_empty() {
                self.last_row_overflows_prev_block = Some(current_row);
                return Ok(RowBlock::Rows(rows_to_return));
            }
            if current_row_size_bytes < self.block_size_bytes && (row_size_bytes_to_return + current_row_size_bytes > self.block_size_bytes) {
                self.last_row_overflows_prev_block = Some(current_row);
                return Ok(RowBlock::Rows(rows_to_return));
            }

            row_size_bytes_to_return += current_row_size_bytes;
            rows_to_return.push(current_row);
        }

        Ok(RowBlock::Rows(rows_to_return))
    }

    fn next_row(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match self.last_row_overflows_prev_block.take() {
            Some(prev_row) => Ok(Some(prev_row)),
            None => self.inner_iterator.next(),
        }
    }
}