use crate::sql::execution::sort::sort_page::SortPage;
use shared::SimpleDbError::{CannotReadSortFile, CannotWriteSortFile};
use shared::{SimpleDbError, SimpleDbFile, SimpleDbOptions};
use std::sync::Arc;
use storage::TemporarySpace;
use crate::{Row, Schema};

#[derive(Clone)]
pub struct SortFiles {
    temporary_space: TemporarySpace,
    options: Arc<SimpleDbOptions>,
    table_schema: Schema,
    initialized: bool,

    output: Option<SimpleDbFile>,
    input: Option<SimpleDbFile>,
}

//This iterator will be used by Sorter. This iterator acts like a fixed window iterator
//'a lives as long as SortFiles lives
//k indicates how many sort pages we will jump, when the user calls next()
pub struct SortFilePageIterator<'a> {
    table_schema: Schema,

    sort_page_size_bytes: usize,
    file: &'a SimpleDbFile,
    k: usize,

    n_pages_returned_in_run: usize,
    current_offset_left: usize,
    current_offset_right: usize,
}

impl SortFiles {
    pub fn create(
        temporary_space: TemporarySpace,
        options: Arc<SimpleDbOptions>,
        table_schema: Schema
    ) -> Result<SortFiles, SimpleDbError> {
        Ok(SortFiles {
            initialized: false,
            temporary_space,
            output: None,
            table_schema,
            input: None,
            options
        })
    }

    pub fn input_iterator(&self, k: usize) -> SortFilePageIterator {
        SortFilePageIterator::create(
            self.input.as_ref().unwrap(),
            self.options.sort_page_size_bytes,
            &self.table_schema,
            k
        )
    }

    pub fn swap_input_output_files(&mut self) {
        let output = self.output.take().unwrap();
        let input = self.input.take().unwrap();
        self.input = Some(output);
        self.output = Some(input);
    }

    pub fn write_sort_page_to_output(&mut self, page: SortPage) -> Result<(), SimpleDbError> {
        self.maybe_initialize()?;

        self.output
            .as_mut()
            .unwrap()
            .write(&page.serialize(self.options.sort_page_size_bytes))
            .map_err(|e| CannotWriteSortFile(e))?;
        Ok(())
    }

    pub fn write_sort_page_to_input(&mut self, page: SortPage) -> Result<(), SimpleDbError> {
        self.maybe_initialize()?;

        self.input
            .as_mut()
            .unwrap()
            .write(&page.serialize(self.options.sort_page_size_bytes))
                .map_err(|e| CannotWriteSortFile(e))?;
        Ok(())
    }

    fn maybe_initialize(&mut self) -> Result<(), SimpleDbError> {
        if !self.initialized {
            self.output = Some(self.temporary_space.create_file("output")?);
            self.input = Some(self.temporary_space.create_file("input")?);
            self.initialized = false;
        }

        Ok(())
    }
}

impl<'a> SortFilePageIterator<'a> {
    pub fn create(
        file: &'a SimpleDbFile,
        sort_page_size_bytes: usize,
        schema: &Schema,
        k: usize,
    ) -> SortFilePageIterator<'a> {
        SortFilePageIterator {
            current_offset_right: sort_page_size_bytes * k,
            table_schema: schema.clone(),
            n_pages_returned_in_run: 0,
            current_offset_left: 0,
            sort_page_size_bytes,
            file,
            k,
        }
    }

    pub fn next_left(&mut self) -> Result<Option<Vec<Row>>, SimpleDbError> {
        match self.read_row(self.current_offset_left)? {
            Some((row, new_offset)) => {
                self.current_offset_left = new_offset;
                Ok(Some(row))
            }
            None => Ok(None)
        }
    }

    pub fn next_right(&mut self) -> Result<Option<Vec<Row>>, SimpleDbError> {
        match self.read_row(self.current_offset_right)? {
            Some((row, new_offset)) => {
                self.current_offset_right = new_offset;
                Ok(Some(row))
            }
            None => Ok(None)
        }
    }

    pub fn has_next(&self) -> bool {
        self.current_offset_left < self.file.size() || self.current_offset_right < self.file.size()
    }

    //Returns row and the new offset to read from the file.
    fn read_row(
        &self,
        offset: usize
    ) -> Result<Option<(Vec<Row>, usize)>, SimpleDbError> {
        if offset >= self.file.size() {
            return Ok(None);
        }

        let mut left_sort_page_bytes = self.file.read(offset, self.sort_page_size_bytes)
            .map_err(|e| CannotReadSortFile(e))?;
        let first_page = SortPage::deserialize(&mut left_sort_page_bytes.as_slice(), self.sort_page_size_bytes);

        if first_page.is_normal_page() {
            let rows = first_page.deserialize_rows(&self.table_schema);
            return Ok(Some((rows, offset + self.sort_page_size_bytes)));
        } else {
            let (overflow_row, new_offset) = self.read_overflow_page(first_page, offset)?;
            Ok(Some((vec![overflow_row], new_offset)))
        }
    }

    fn read_overflow_page(
        &self,
        first_page: SortPage,
        first_page_offset: usize
    ) -> Result<(Row, usize), SimpleDbError> {
        let mut current_offset = first_page_offset + self.sort_page_size_bytes;
        let mut row_bytes = first_page.row_bytes();

        loop {
            let mut left_sort_page_bytes = self.file.read(current_offset, self.sort_page_size_bytes)
                .map_err(|e| CannotReadSortFile(e))?;
            let current_page = SortPage::deserialize(&mut left_sort_page_bytes.as_slice(), self.sort_page_size_bytes);

            current_offset += self.sort_page_size_bytes;
            row_bytes.extend(current_page.row_bytes());

            if current_page.is_last_overflow_page() {
                row_bytes.extend(current_page.row_bytes());
                let row = Row::deserialize(&mut row_bytes.as_slice(), &self.table_schema);
                return Ok((row, current_offset))
            }
        }
    }
}