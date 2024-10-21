use crate::selection::Selection;
use crate::sql::execution::sort::sort_page::SortPage;
use crate::sql::plan::plan_step::PlanStep;
use crate::sql::query_iterator::{BlockQueryIterator, RowBlock};
use crate::table::table::Table;
use crate::{QueryIterator, Row, Sort, SortOrder};
use bytes::{Buf, BufMut};
use shared::SimpleDbError::CannotWriteSortFile;
use shared::{SimpleDbError, SimpleDbFile, SimpleDbOptions};
use std::sync::Arc;
use storage::TemporarySpace;

#[derive(Clone)]
pub struct Sorter {
    temporary_space: TemporarySpace,

    options: Arc<SimpleDbOptions>,
    table: Arc<Table>,
    source: PlanStep,
    sort: Sort,
}

impl Sorter {
    pub fn create(
        options: Arc<SimpleDbOptions>,
        table: Arc<Table>,
        source: PlanStep,
        sort: Sort,
    ) -> Result<Sorter, SimpleDbError> {
        Ok(Sorter {
            temporary_space: table.storage.create_temporary_space()?,
            options,
            source,
            table,
            sort
        })
    }

    pub fn sort(
        &mut self,
    ) -> Result<QueryIterator, SimpleDbError> {
        let mut output = self.temporary_space.create_file("output")?;
        let mut input = self.temporary_space.create_file("input")?;

        self.pass1(&mut input);

        todo!()
    }

    //In pass one we split the rows by pages, sort them and store them in the page.
    fn pass1(
        &mut self,
        input: &mut SimpleDbFile
    ) -> Result<(), SimpleDbError> {
        let mut query_iterator = BlockQueryIterator::create(self.row_bytes_per_sort_page(), QueryIterator::create(
            Selection::All, self.source.clone(), self.table.get_schema().clone()
        ));

        while let block_of_rows = query_iterator.next_block()? {
            match block_of_rows {
                RowBlock::Overflow(overflow_row) => self.write_overflow_row_pages_to_input(input, overflow_row)?,
                RowBlock::Rows(rows) => self.write_normal_row_pages_to_input(input, rows)?
            };
        }

        Ok(())
    }

    fn write_overflow_row_pages_to_input(
        &self,
        input: &mut SimpleDbFile,
        overflow_row: Row
    ) -> Result<(), SimpleDbError> {
        let row_serialized = overflow_row.serialize();
        let mut current_ptr = &mut row_serialized.as_slice();
        let mut pages = Vec::new();
        let n_pages = (row_serialized.len() / self.row_bytes_per_sort_page()) + 1;

        for (current_index, _) in (0..n_pages).enumerate() {
            if current_index < n_pages {
                let row_bytes = current_ptr[..self.row_bytes_per_sort_page()].to_vec();
                pages.push(SortPage::create_next_page_overflow(row_bytes, 1));
                current_ptr.advance(self.row_bytes_per_sort_page());
            } else {
                let row_bytes = current_ptr.to_vec();
                pages.push(SortPage::create_last_page_overflow(row_bytes, 1));
            }
        }

        for page in pages {
            input.write(&page.serialize(self.options.sort_page_size_bytes))
                .map_err(|e| CannotWriteSortFile(e))?;
        }

        Ok(())
    }

    fn write_normal_row_pages_to_input(
        &self,
        input: &mut SimpleDbFile,
        mut rows: Vec<Row>
    ) -> Result<(), SimpleDbError> {
        if rows.len() > 0 {
            self.sort_rows(&mut rows);

            let n_rows = rows.len();
            let serialized_rows = Self::serialize_rows(rows);
            let sort_page = SortPage::create_normal(serialized_rows, n_rows);
            input.write(&sort_page.serialize(self.options.sort_page_size_bytes))
                .map_err(|e| CannotWriteSortFile(e))?;
        }

        Ok(())
    }

    fn serialize_rows(rows: Vec<Row>) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();
        serialized.put_u32(rows.len() as u32);
        for row in rows {
            serialized.extend(row.serialize());
        }
        serialized
    }

    fn row_bytes_per_sort_page(&self) -> usize {
        self.options.sort_page_size_bytes - SortPage::header_size_bytes()
    }

    fn sort_rows(&self, rows: &mut Vec<Row>) {
        rows.sort_by(|a, b| {
            let value_a = a.get_column_value(&self.sort.column_name).unwrap();
            let value_b = b.get_column_value(&self.sort.column_name).unwrap();

            match self.sort.order {
                SortOrder::Desc => value_b.cmp(&value_a),
                SortOrder::Asc => value_a.cmp(&value_b)
            }
        });
    }
}