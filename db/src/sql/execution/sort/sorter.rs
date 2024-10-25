use crate::sql::execution::sort::sort_files::SortFiles;
use crate::sql::execution::sort::sort_page::SortPage;
use crate::sql::execution::sort::sorted_result_iterator::SortedResultIterator;
use crate::sql::plan::plan_step::PlanStep;
use crate::table::block_row_iterator::{RowBlock, RowBlockIterator};
use crate::table::row::MockRowIterator;
use crate::table::selection::Selection;
use crate::table::table::Table;
use crate::{QueryIterator, Row, Sort, SortOrder};
use bytes::Buf;
use shared::{SimpleDbError, SimpleDbOptions};
use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::sync::Arc;

//Used in write_normal_row_pages() and write_overflow_row_pages() methods
const WRITE_TO_OUTPUT: bool = false;
const WRITE_TO_INPUT: bool = true;

pub struct Sorter {
    //We need to wrap it with UnsafeCell because in pass_n() method, we need to take a mutable reference
    //(to write to the output file) and an immutable reference to iterate the input file.
    sort_files: UnsafeCell<SortFiles>,

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
            sort_files: UnsafeCell::new(SortFiles::create(
                table.storage.create_temporary_space()?,
                options.clone(),
                table.get_schema().clone()
            )?),
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
        println!("Hola");

        let n_pages_written_pass1 = self.pass_0()?;
        let n_total_passess = self.calculate_n_total_passes(n_pages_written_pass1);

        for n_pass in 1..n_total_passess {
            if n_pass == 1 {
                self.pass_1()?;
            } else {
                self.pass_n(n_pass)?;
            }

            self.get_sort_files().swap_input_output_files();
        }

        self.get_sort_files().swap_input_output_files();

        Ok(QueryIterator::create(
            self.selection.clone(),
            SortedResultIterator::create(self.get_sort_files().take_output_file(), self.table.clone()),
            self.table.get_schema().clone()
        ))
    }

    fn pass_n (
        &mut self,
        n_pass: usize
    ) -> Result<(), SimpleDbError> {
        let mut input_iterator = self.get_sort_files().input_iterator(n_pass)?;
        let mut output_buffer: Vec<Row> = Vec::new();
        let mut buffer_right: Vec<Row> = Vec::new();
        let mut buffer_left: Vec<Row> = Vec::new();
        let mut current_size_bytes_output_buffer = 0;

        while input_iterator.has_next() {
            if buffer_left.is_empty() {
                buffer_left = input_iterator.next_left()?.unwrap_or(Vec::new());
            }
            if buffer_right.is_empty() {
                buffer_right = input_iterator.next_right()?.unwrap_or(Vec::new());
            }

            match self.take_min(&mut buffer_left, &mut buffer_right) {
                Some(min_row) => {
                    let min_row_size_bytes = min_row.serialized_size();

                    if min_row_size_bytes > self.row_bytes_per_sort_page() {
                        //This function will clear the output buffer
                        self.write_normal_row_pages(&mut output_buffer, WRITE_TO_OUTPUT);
                        self.write_overflow_row_pages(min_row, WRITE_TO_OUTPUT);
                        current_size_bytes_output_buffer = 0;

                    } else if current_size_bytes_output_buffer + min_row_size_bytes > self.row_bytes_per_sort_page() {
                        self.write_normal_row_pages(&mut output_buffer, WRITE_TO_OUTPUT);
                        current_size_bytes_output_buffer = min_row_size_bytes;
                        output_buffer.push(min_row);

                    } else if current_size_bytes_output_buffer + min_row_size_bytes <= self.row_bytes_per_sort_page() {
                        output_buffer.push(min_row);
                        current_size_bytes_output_buffer += min_row_size_bytes;
                    }
                },
                None => {}
            }
        }

        Ok(())
    }

    //In pass one we split the rows by pages, sort them and store them in the page.
    //This function returns the number of pages written. (Overflow pages only count for 1 page written)
    fn pass_0(
        &mut self,
    ) -> Result<usize, SimpleDbError> {
        let mut query_iterator = RowBlockIterator::create(self.row_bytes_per_sort_page(), QueryIterator::create(
            Selection::All, self.source.clone(), self.table.get_schema().clone()
        ));
        let mut n_pages_written = 0;

        while let Some(block_of_rows) = query_iterator.next_block()? {
            n_pages_written += 1;
            match block_of_rows {
                RowBlock::Overflow(overflow_row) => self.write_overflow_row_pages(overflow_row, WRITE_TO_INPUT)?,
                RowBlock::Rows(mut rows) => {
                    self.sort_rows(&mut rows);
                    self.write_normal_row_pages(&mut rows, WRITE_TO_INPUT)?
                }
            };
        }

        Ok(n_pages_written)
    }

    fn pass_1(&mut self) -> Result<(), SimpleDbError> {
        let mut input_iterator = self.get_sort_files().input_iterator(1)?;
        let mut buffer_right: Option<Vec<Row>> = None;
        let mut buffer_left: Option<Vec<Row>> = None;

        while input_iterator.has_next() {
            //Load left & right buffers
            match (&buffer_left, &buffer_right) {
                (Some(left), None) => {
                    buffer_right = input_iterator.next_right()?;
                },
                (None, Some(right)) => {
                    buffer_left = input_iterator.next_left()?;
                },
                (None, None) => {
                    buffer_right = input_iterator.next_right()?;
                    buffer_left = input_iterator.next_left()?;
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
                    RowBlock::Overflow(overflow_row) => self.write_overflow_row_pages(overflow_row, WRITE_TO_OUTPUT)?,
                    RowBlock::Rows(mut rows) => self.write_normal_row_pages(&mut rows, WRITE_TO_OUTPUT)?,
                }
            }
        }

        Ok(())
    }

    fn write_overflow_row_pages(
        &self,
        overflow_row: Row,
        write_to_input_file: bool
    ) -> Result<(), SimpleDbError> {
        let row_serialized = overflow_row.serialize();
        let mut current_ptr = &mut row_serialized.as_slice();
        let mut pages = Vec::new();
        let n_pages = (row_serialized.len() / self.row_bytes_per_sort_page()) + 1;

        for (current_index, _) in (0..n_pages).enumerate() {
            if current_index == 0 {
                let row_bytes = current_ptr[..self.row_bytes_per_sort_page()].to_vec();
                pages.push(SortPage::create_first_page_overflow(row_bytes, 1));
                current_ptr.advance(self.row_bytes_per_sort_page());
            } else if current_index < n_pages {
                let row_bytes = current_ptr[..self.row_bytes_per_sort_page()].to_vec();
                pages.push(SortPage::create_next_page_overflow(row_bytes, 1));
                current_ptr.advance(self.row_bytes_per_sort_page());
            } else {
                let row_bytes = current_ptr.to_vec();
                pages.push(SortPage::create_last_page_overflow(row_bytes, 1));
            }
        }

        for page in pages {
            if write_to_input_file {
                self.get_sort_files()
                    .write_sort_page_to_input(page)?;
            } else {
                self.get_sort_files()
                    .write_sort_page_to_output(page)?;
            }
        }

        Ok(())
    }

    fn write_normal_row_pages(
        &self,
        rows: &mut Vec<Row>,
        write_to_input_file: bool
    ) -> Result<(), SimpleDbError> {
        if rows.len() > 0 {
            let n_rows = rows.len();
            let serialized_rows = Self::serialize_rows(rows);
            let sort_page = SortPage::create_normal(serialized_rows, n_rows);

            if write_to_input_file {
                self.get_sort_files()
                    .write_sort_page_to_input(sort_page)?;
            } else {
                self.get_sort_files()
                    .write_sort_page_to_output(sort_page)?;
            }
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
            Self::compare_row(a, b, &self.sort)
        });
    }

    fn compare_row(a: &Row, b: &Row, sort: &Sort) -> Ordering {
        let value_a = a.get_column_value(&sort.column_name).unwrap();
        let value_b = b.get_column_value(&sort.column_name).unwrap();

        match sort.order {
            SortOrder::Desc => value_b.cmp(&value_a),
            SortOrder::Asc => value_a.cmp(&value_b)
        }
    }

    fn calculate_n_total_passes(&self, n_pages_written_pass1: usize) -> usize {
        (n_pages_written_pass1 as f64).log2().ceil() as usize
    }

    fn get_sort_files(&self) -> &mut SortFiles {
        unsafe { &mut (*self.sort_files.get()) }
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

        match Self::compare_row(left_value, right_value, &self.sort) {
            Ordering::Less |
            Ordering::Equal => Some(left_vec.remove(0)),
            Ordering::Greater => Some(right_vec.remove(0))
        }
    }
}

impl Clone for Sorter {
    fn clone(&self) -> Self {
        Sorter {
            sort_files: UnsafeCell::new(unsafe { (*self.sort_files.get()).clone() }),
            selection: self.selection.clone(),
            options: self.options.clone(),
            table: self.table.clone(),
            source: self.source.clone(),
            sort: self.sort.clone()
        }
    }
}