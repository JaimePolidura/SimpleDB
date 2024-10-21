use crate::selection::Selection;
use crate::sql::execution::sort::sort_files::SortFiles;
use crate::sql::execution::sort::sort_page::SortPage;
use crate::sql::plan::plan_step::PlanStep;
use crate::sql::query_iterator::{BlockQueryIterator, RowBlock};
use crate::table::table::Table;
use crate::{QueryIterator, Row, Sort, SortOrder};
use bytes::{Buf, BufMut};
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;

#[derive(Clone)]
pub struct Sorter {
    sort_files: SortFiles,

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
            sort_files: SortFiles::create(
                table.storage.create_temporary_space()?,
                options.clone(),
                table.get_schema().clone()
            )?,
            options,
            source,
            table,
            sort
        })
    }

    pub fn sort(
        &mut self,
    ) -> Result<QueryIterator, SimpleDbError> {
        let n_pages_written_pass1 = self.pass1()?;

        for n_pass in 0..self.calculate_n_total_passes(n_pages_written_pass1) {

        }

        todo!()
    }

    //In pass one we split the rows by pages, sort them and store them in the page.
    //This function returns the number of pages written. (Overflow pages only count for 1 page written)
    fn pass1(
        &mut self,
    ) -> Result<usize, SimpleDbError> {
        let mut query_iterator = BlockQueryIterator::create(self.row_bytes_per_sort_page(), QueryIterator::create(
            Selection::All, self.source.clone(), self.table.get_schema().clone()
        ));
        let mut n_pages_written = 0;

        while let block_of_rows = query_iterator.next_block()? {
            n_pages_written += 1;
            match block_of_rows {
                RowBlock::Overflow(overflow_row) => self.write_overflow_row_pages_to_input(overflow_row)?,
                RowBlock::Rows(rows) => self.write_normal_row_pages_to_input(rows)?
            };
        }

        Ok(n_pages_written)
    }

    fn write_overflow_row_pages_to_input(
        &mut self,
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
            self.sort_files.write_sort_page_to_input(page)?;
        }

        Ok(())
    }

    fn write_normal_row_pages_to_input(
        &mut self,
        mut rows: Vec<Row>
    ) -> Result<(), SimpleDbError> {
        if rows.len() > 0 {
            self.sort_rows(&mut rows);

            let n_rows = rows.len();
            let serialized_rows = Self::serialize_rows(rows);
            let sort_page = SortPage::create_normal(serialized_rows, n_rows);

            self.sort_files.write_sort_page_to_input(sort_page)?;
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

    fn calculate_n_total_passes(&self, n_pages_written_pass1: usize) -> usize {
        (n_pages_written_pass1 as f64).log2().ceil() as usize
    }
}