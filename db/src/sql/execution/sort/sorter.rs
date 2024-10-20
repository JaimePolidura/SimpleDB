use std::sync::Arc;
use bytes::{Buf, BufMut};
use crate::{QueryIterator, Row, Sort};
use shared::{utils, SimpleDbError, SimpleDbFile, SimpleDbOptions};
use shared::SimpleDbError::CannotWriteSortFile;
use storage::TemporarySpace;
use crate::selection::Selection;
use crate::sql::execution::sort::sort_page::SortPage;
use crate::sql::plan::plan_step::PlanStep;
use crate::table::table::Table;

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
    fn pass1(&mut self, input: &mut SimpleDbFile) -> Result<(), SimpleDbError> {
        let mut query_iterator = QueryIterator::create(
            Selection::All, self.source.clone(), self.table.get_schema().clone()
        );

        while let Some((mut rows, is_overflow)) = query_iterator.next_bytes(self.row_bytes_per_sort_page())? {
            if is_overflow {
                let overflow_row = rows.pop().unwrap();
                self.write_normal_row_pages_to_input(input, rows)?;
                self.write_overflow_row_pages_to_input(input, overflow_row)?
            } else {
                self.write_normal_row_pages_to_input(input, rows)?;
            }
        }

        Ok(())
    }

    fn write_overflow_row_pages_to_input(&self, input: &mut SimpleDbFile, overflow_row: Row) -> Result<(), SimpleDbError> {
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
            input.write(&page.serialize())
                .map_err(|e| CannotWriteSortFile(e))?;
        }

        Ok(())
    }

    fn write_normal_row_pages_to_input(&self, input: &mut SimpleDbFile, mut rows: Vec<Row>) -> Result<(), SimpleDbError> {
        if rows.len() > 0 {
            self.sort_rows(&mut rows);

            let n_rows = rows.len();
            let serialized_rows = Self::serialize_rows(rows);
            let sort_page = SortPage::create_normal(serialized_rows, n_rows);
            input.write(&sort_page.serialize())
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
        todo!()
    }
}