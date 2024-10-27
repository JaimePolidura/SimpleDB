use crate::sql::execution::sort::sort_file::SortFile;
use crate::sql::execution::sort::sort_page::SortPage;
use crate::sql::execution::sort::sort_page_run_iterator::SortPageRunIterator;
use crate::sql::execution::sort::sorted_result_iterator::SortedResultIterator;
use crate::sql::plan::plan_step::PlanStep;
use crate::table::block_row_iterator::{RowBlock, RowBlockIterator};
use crate::table::row::MockRowIterator;
use crate::table::selection::Selection;
use crate::table::table::Table;
use crate::{QueryIterator, Row, Sort};
use bytes::Buf;
use shared::{SimpleDbError, SimpleDbFileMode, SimpleDbOptions};
use std::cmp::Ordering;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use storage::TemporarySpace;

//Used in write_normal_row_pages() and write_overflow_row_pages() methods
const WRITE_TO_OUTPUT: bool = false;
const WRITE_TO_INPUT: bool = true;

pub struct Sorter {
    //We need to wrap it with UnsafeCell because in pass_n() method, we need to take a mutable reference
    //(to write to the output file) and an immutable reference to iterate the input file.
    last_file_id: AtomicUsize,

    temporary_space: Arc<TemporarySpace>,
    options: Arc<SimpleDbOptions>,
    selection: Selection,
    table: Arc<Table>,
    source: PlanStep,
    sort: Sort,
}

impl Sorter {
    pub fn create(
        options: Arc<SimpleDbOptions>,
        selection: Selection,
        table: Arc<Table>,
        source: PlanStep,
        sort: Sort,
    ) -> Result<Sorter, SimpleDbError> {
        Ok(Sorter {
            temporary_space: Arc::new(table.storage.create_temporary_space()?),
            last_file_id: AtomicUsize::new(0),
            selection,
            options,
            source,
            table,
            sort
        })
    }

    pub fn sort(
        &mut self,
    ) -> Result<QueryIterator<SortedResultIterator>, SimpleDbError> {
        let mut output = self.create_next_sort_file()?;
        let mut input = self.create_next_sort_file()?;

        let n_pages_written_pass1 = self.pass_0(&mut output)?; //Writes to input
        let n_total_passess = self.calculate_n_total_passes(n_pages_written_pass1);

        let mut prev_n_pages_per_run = 1;

        for n_pass in 1..(n_total_passess + 1) {
            //Every pass writes to output
            input = output;
            output = self.create_next_sort_file()?;

            if n_pass == 1 {
                self.pass_1(&input, &mut output)?;
            } else {
                prev_n_pages_per_run = prev_n_pages_per_run * 2;
                self.pass_n(prev_n_pages_per_run, &input, &mut output)?;
            }
        }

        Ok(QueryIterator::create(
            self.selection.clone(),
            SortedResultIterator::create(output, self.table.clone()),
            self.table.get_schema().clone()
        ))
    }

    fn pass_n (
        &mut self,
        n_pages_per_run: usize,
        input: &SortFile,
        output: &mut SortFile,
    ) -> Result<(), SimpleDbError> {
        let mut current_size_bytes_output_buffer = 0;
        let mut output_buffer: Vec<Row> = Vec::new();
        let mut buffer_right: Vec<Row> = Vec::new();
        let mut buffer_left: Vec<Row> = Vec::new();
        let mut input_iterator = SortPageRunIterator::create(
            input.clone(),
            self.options.sort_page_size_bytes,
            self.table.get_schema(),
            n_pages_per_run
        )?;

        while input_iterator.has_next() || !buffer_left.is_empty() || !buffer_right.is_empty() {
            if buffer_left.is_empty() {
                buffer_left = input_iterator.next_left()?.unwrap_or(Vec::new());
            }
            if buffer_right.is_empty() {
                buffer_right = input_iterator.next_right()?.unwrap_or(Vec::new());
            }
            if buffer_right.is_empty() && buffer_left.is_empty() {
                break;
            }

            match self.take_min(&mut buffer_left, &mut buffer_right) {
                Some(min_row) => {
                    let current_min_row_size_bytes = min_row.serialized_size();

                    if current_min_row_size_bytes > self.row_bytes_per_sort_page() {
                        //This function will clear the output buffer
                        self.write_normal_row_pages(output, &mut output_buffer);
                        self.write_overflow_row_pages(output, min_row);
                        current_size_bytes_output_buffer = 0;

                    } else if current_size_bytes_output_buffer + current_min_row_size_bytes > self.row_bytes_per_sort_page() {
                        self.write_normal_row_pages(output, &mut output_buffer);
                        current_size_bytes_output_buffer = current_min_row_size_bytes;
                        output_buffer.push(min_row);

                    } else if current_size_bytes_output_buffer + current_min_row_size_bytes <= self.row_bytes_per_sort_page() {
                        output_buffer.push(min_row);
                        current_size_bytes_output_buffer += current_min_row_size_bytes;
                    }
                },
                None => {}
            }
        }

        if !output_buffer.is_empty() {
            self.write_normal_row_pages(output, &mut output_buffer);
        }

        Ok(())
    }

    //In pass one we split the rows by pages, sort them and store them in the page.
    //This function returns the number of pages written. (Overflow pages only count for 1 page written)
    fn pass_0(
        &mut self,
        file_input: &mut SortFile
    ) -> Result<usize, SimpleDbError> {
        let mut query_iterator = RowBlockIterator::create(self.row_bytes_per_sort_page(), QueryIterator::create(
            Selection::All, self.source.clone(), self.table.get_schema().clone()
        ));
        let mut n_pages_written = 0;

        while let Some(block_of_rows) = query_iterator.next_block()? {
            n_pages_written += 1;
            match block_of_rows {
                RowBlock::Overflow(overflow_row) => self.write_overflow_row_pages(file_input, overflow_row)?,
                RowBlock::Rows(mut rows) => {
                    self.sort_rows(&mut rows);
                    self.write_normal_row_pages(file_input, &mut rows)?
                }
            };
        }

        Ok(n_pages_written)
    }

    fn pass_1(&mut self, input: &SortFile, output: &mut SortFile) -> Result<(), SimpleDbError> {
        let mut buffer_right: Option<Vec<Row>> = None;
        let mut buffer_left: Option<Vec<Row>> = None;
        let mut input_iterator = SortPageRunIterator::create(
            input.clone(),
            self.options.sort_page_size_bytes,
            &self.table.get_schema(),
            1
        )?;

        while input_iterator.has_next() {
            //Load left & right buffers
            match (&buffer_left, &buffer_right) {
                (Some(_), None) => {
                    buffer_right = input_iterator.next_right()?;
                },
                (None, Some(_)) => {
                    buffer_left = input_iterator.next_left()?;
                },
                (None, None) => {
                    buffer_left = input_iterator.next_left()?;
                    buffer_right = input_iterator.next_right()?;
                },
                _ => panic!("Illegal code path")
            }

            //Sort left & right buffers
            let rows_right = buffer_right.take().unwrap_or(Vec::new());
            let rows_left = buffer_left.take().unwrap_or(Vec::new());
            let mut sorted_rows = Vec::new();
            sorted_rows.extend(rows_left);
            sorted_rows.extend(rows_right);
            self.sort_rows(&mut sorted_rows);

            //Write to output
            let mut rows_iterator = RowBlockIterator::<MockRowIterator>::create_from_vec(
                self.row_bytes_per_sort_page(), sorted_rows
            );
            while let Some(block_of_rows) = rows_iterator.next_block()? {
                match block_of_rows {
                    RowBlock::Overflow(overflow_row) => {
                        self.write_overflow_row_pages(output, overflow_row)?
                    },
                    RowBlock::Rows(mut rows) => {
                        self.write_normal_row_pages(output, &mut rows)?
                    },
                }
            }
        }

        Ok(())
    }

    fn write_overflow_row_pages(
        &self,
        file: &mut SortFile,
        overflow_row: Row,
    ) -> Result<(), SimpleDbError> {
        let row_serialized = overflow_row.serialize();
        let mut current_ptr = &mut row_serialized.as_slice();
        let n_pages = (row_serialized.len() / self.row_bytes_per_sort_page()) + 1;

        for (current_index, _) in (0..n_pages).enumerate() {
            if current_index == 0 {
                let row_bytes = current_ptr[..self.row_bytes_per_sort_page()].to_vec();
                file.write(SortPage::create_first_page_overflow(row_bytes, 1));
                current_ptr.advance(self.row_bytes_per_sort_page());
            } else if current_index < n_pages {
                let row_bytes = current_ptr[..self.row_bytes_per_sort_page()].to_vec();
                file.write(SortPage::create_next_page_overflow(row_bytes, 1));
                current_ptr.advance(self.row_bytes_per_sort_page());
            } else {
                let row_bytes = current_ptr.to_vec();
                file.write(SortPage::create_last_page_overflow(row_bytes, 1));
            }
        }

        Ok(())
    }

    fn write_normal_row_pages(
        &self,
        file: &mut SortFile,
        rows: &mut Vec<Row>,
    ) -> Result<(), SimpleDbError> {
        if rows.len() > 0 {
            let n_rows = rows.len();
            let serialized_rows = Self::serialize_rows(rows);
            let sort_page = SortPage::create_normal(serialized_rows, n_rows);

            file.write(sort_page)?;
        }

        Ok(())
    }

    fn serialize_rows(rows: &mut Vec<Row>) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();
        while !rows.is_empty() {
            serialized.extend(rows.remove(0).serialize());
        }

        serialized
    }

    fn row_bytes_per_sort_page(&self) -> usize {
        self.options.sort_page_size_bytes - SortPage::header_size_bytes()
    }

    fn sort_rows(&self, rows: &mut Vec<Row>) {
        rows.sort_by(|a, b| {
            self.sort.compare(a, b)
        });
    }

    fn calculate_n_total_passes(&self, n_pages_written_pass1: usize) -> usize {
        (n_pages_written_pass1 as f64).log2().ceil() as usize
    }

    fn take_min(
        &self,
        left_vec: &mut Vec<Row>,
        right_vec: &mut Vec<Row>,
    ) -> Option<Row> {
        if left_vec.is_empty() && right_vec.is_empty() {
            return None;
        }
        if left_vec.is_empty() {
            return Some(right_vec.remove(0));
        }
        if right_vec.is_empty() {
            return Some(left_vec.remove(0));
        }

        let right_value = right_vec.get(0).unwrap();
        let left_value = left_vec.get(0).unwrap();

        match self.sort.compare(left_value, right_value) {
            Ordering::Less |
            Ordering::Equal => Some(left_vec.remove(0)),
            Ordering::Greater => Some(right_vec.remove(0))
        }
    }

    fn create_next_sort_file(&mut self) -> Result<SortFile, SimpleDbError> {
        let file_id = self.last_file_id.fetch_add(1, Relaxed);
        let file = self.temporary_space.create_file(
            file_id.to_string().as_str(), SimpleDbFileMode::AppendOnly
        )?;
        Ok(SortFile::create(file, self.options.sort_page_size_bytes))
    }
}

impl Clone for Sorter {
    fn clone(&self) -> Self {
        Sorter {
            last_file_id: AtomicUsize::new(self.last_file_id.load(Relaxed)),
            temporary_space: self.temporary_space.clone(),
            selection: self.selection.clone(),
            options: self.options.clone(),
            table: self.table.clone(),
            source: self.source.clone(),
            sort: self.sort.clone(),
        }
    }
}