use crate::selection::Selection;
use crate::sql::plan::plan_step::{PlanStep, PlanStepDesc};
use crate::table::schema::Schema;
use crate::{Column, Row};
use shared::SimpleDbError;

//This will be returned to the user of SimpleDb when it queries data
//This is simple wrapper around a Plan
#[derive(Clone)]
pub struct QueryIterator {
    source: PlanStep,
    schema: Schema,
    selection: Selection,
}

//Iterates a query iterator by returning block of rows whose size is less or equal than block_size_bytes
pub struct BlockQueryIterator {
    inner_iterator: QueryIterator,
    block_size_bytes: usize,

    last_row_overflows_prev_block: Option<Row>,
}

pub enum RowBlock {
    Rows(Vec<Row>), //Rows whose serialized size is less or equal than block size
    Overflow(Row) //This row overflows block size
}

impl QueryIterator {
    pub fn create(
        selection: Selection,
        plan: PlanStep,
        schema: Schema
    ) -> QueryIterator {
        QueryIterator { source: plan, schema, selection }
    }

    pub fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        self.source.next()
    }

    pub fn get_selected_columns(&self) -> Vec<Column> {
        match &self.selection {
            Selection::All => {
                self.schema.get_columns()
            },
            Selection::Some(selected_columns_names) => {
                let mut selected_columns = Vec::new();

                for selected_columns_name in selected_columns_names {
                    let column = self.schema.get_column(selected_columns_name)
                        .unwrap();
                    selected_columns.push(column);
                }

                selected_columns
            }
        }
    }

    pub fn next_n(&mut self, n: usize) -> Result<Vec<Row>, SimpleDbError> {
        let mut results = Vec::new();

        while results.len() <= n {
            match self.source.next()? {
                Some(row) => results.push(row),
                None => break
            };
        }

        Ok(results)
    }

    pub fn all(&mut self) -> Result<Vec<Row>, SimpleDbError> {
        let mut results = Vec::new();

        while let Some(row) = self.source.next()? {
            results.push(row);
        }

        Ok(results)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_plan_desc(&self) -> PlanStepDesc {
        self.source.desc()
    }
}

impl BlockQueryIterator {
    pub fn create(
        block_size: usize,
        inner_iterator: QueryIterator,
    ) -> BlockQueryIterator {
        BlockQueryIterator {
            last_row_overflows_prev_block: None,
            block_size_bytes: block_size,
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