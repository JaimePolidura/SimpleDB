use crate::sql::execution::sort::sort_page::SortPage;
use crate::{Row, Schema};
use shared::SimpleDbError::{CannotReadSortFile, CannotWriteSortFile};
use shared::{SimpleDbError, SimpleDbFile, SimpleDbFileMode, SimpleDbOptions};
use std::sync::Arc;
use storage::TemporarySpace;
use crate::sql::execution::sort::sort_file::SortFile;
use crate::sql::execution::sort::sort_files::SortFilePageIteratorState::LeftAndRightAvailable;

#[derive(Clone)]
pub struct SortFiles {
    temporary_space: TemporarySpace,
    options: Arc<SimpleDbOptions>,
    table_schema: Schema,
    initialized: bool,

    output: Option<SortFile>,
    input: Option<SortFile>,
}

//This iterator will maintain two pointers on the file (left and right) that will be separated by k
//When the pages returned by these pointers reaches K, they (left & right( will be advanced by k pages.
//This will be used by external merge sort algoritm (sorter.rs).
//'a lives as long as SortFiles lives
//k indicates the difference in sort pages that there will be in left & right offsets
//https://www.youtube.com/watch?v=F9XmmS8rL4c&t=698s
pub struct SortFilePageIterator<'a> {
    table_schema: Schema,

    sort_page_size_bytes: usize,
    file: &'a SortFile,
    k: usize,

    state: SortFilePageIteratorState,
    current_offset_left: usize,
    current_offset_right: usize,
    n_pages_returned_right: usize,
}

enum SortFilePageIteratorState {
    LeftAndRightAvailable,
    OnlyLeftAvailable
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

    pub fn input_iterator(&self, k: usize) -> Result<SortFilePageIterator, SimpleDbError> {
        SortFilePageIterator::create(
            self.input.as_ref().unwrap(),
            self.options.sort_page_size_bytes,
            &self.table_schema,
            k
        )
    }

    pub fn take_output_file(&mut self) -> SortFile {
        self.output.take().unwrap()
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
            .write(page)
    }

    pub fn write_sort_page_to_input(&mut self, page: SortPage) -> Result<(), SimpleDbError> {
        self.maybe_initialize()?;

        self.input
            .as_mut()
            .unwrap()
            .write(page)
    }

    fn maybe_initialize(&mut self) -> Result<(), SimpleDbError> {
        if !self.initialized {
            self.output = Some(SortFile::create(
                self.temporary_space.create_file("output", SimpleDbFileMode::AppendOnly)?,
                self.options.sort_page_size_bytes)
            );
            self.input = Some(SortFile::create(
                self.temporary_space.create_file("input", SimpleDbFileMode::AppendOnly)?,
                self.options.sort_page_size_bytes)
            );
            self.initialized = false;
        }

        Ok(())
    }
}

impl<'a> SortFilePageIterator<'a> {
    pub fn create(
        file: &'a SortFile,
        sort_page_size_bytes: usize,
        schema: &Schema,
        k: usize,
    ) -> Result<SortFilePageIterator<'a>, SimpleDbError> {
        let mut iterator = SortFilePageIterator {
            state: LeftAndRightAvailable,
            table_schema: schema.clone(),
            current_offset_right: 0, //Will get init when calling next_right()
            n_pages_returned_right: 0,
            current_offset_left: 0,
            sort_page_size_bytes,
            file,
            k,
        };

        iterator.initialize()?;

        Ok(iterator)
    }

    //Expect next_left() and next_right() to be called at the same time
    pub fn next_left(&mut self) -> Result<Option<Vec<Row>>, SimpleDbError> {
        match self.read_row(self.current_offset_left)? {
            Some((row, new_offset)) => {
                self.current_offset_left = new_offset;
                Ok(Some(row))
            }
            None => Ok(None)
        }
    }

    //Expect next_left() and next_right() to be called at the same time
    //Expect call next_right() after next_left()
    pub fn next_right(&mut self) -> Result<Option<Vec<Row>>, SimpleDbError> {
        self.maybe_move_left_and_right_offsets()?;
        self.maybe_initialize_right_offset()?;

        match self.state {
            SortFilePageIteratorState::LeftAndRightAvailable => {
                match self.read_row(self.current_offset_right)? {
                    Some((row, new_offset)) => {
                        self.current_offset_right = new_offset;
                        self.n_pages_returned_right += 1;
                        Ok(Some(row))
                    }
                    None => Ok(None)
                }
            }
            SortFilePageIteratorState::OnlyLeftAvailable => {
                Ok(None)
            }
        }
    }

    pub fn has_next(&self) -> bool {
        self.current_offset_left < self.file.size() || self.current_offset_right < self.file.size()
    }

    fn maybe_move_left_and_right_offsets(&mut self) -> Result<(), SimpleDbError> {
        if self.n_pages_returned_right == self.k {
            self.n_pages_returned_right = 0;
            self.current_offset_left = self.current_offset_right;

            match self.file.get_next_page_offset(
                self.current_offset_right + (self.sort_page_size_bytes * self.k)
            )? {
                Some(new_right_offset) => {
                    self.state = SortFilePageIteratorState::LeftAndRightAvailable;
                    self.current_offset_right = new_right_offset;
                },
                None => self.state = SortFilePageIteratorState::OnlyLeftAvailable,
            };
        }

        Ok(())
    }

    //Returns row and the new offset to read from the file.
    fn read_row(
        &self,
        offset: usize
    ) -> Result<Option<(Vec<Row>, usize)>, SimpleDbError> {
        if offset >= self.file.size() {
            return Ok(None);
        }

        let (row_bytes, n_rows, next_offset) = self.file.read_row_bytes(offset)?.unwrap();
        let rows = Row::deserialize_rows(&row_bytes, n_rows, &self.table_schema);
        return Ok(Some((rows, next_offset)));
    }

    fn initialize(&mut self) -> Result<(), SimpleDbError> {
        match self.file.get_next_page_offset(self.current_offset_left * self.k)? {
            Some(right_offset) => {
                self.state = SortFilePageIteratorState::LeftAndRightAvailable;
                self.current_offset_right = right_offset;
            },
            None => {
                self.state = SortFilePageIteratorState::OnlyLeftAvailable;
            }
        }

        Ok(())
    }

    fn maybe_initialize_right_offset(&mut self) -> Result<(), SimpleDbError> {
        //Already initialized
        if self.current_offset_right != 0 {
            return Ok(());
        }
        //File too small
        if self.k * self.sort_page_size_bytes >= self.file.size() {
            self.state = SortFilePageIteratorState::OnlyLeftAvailable;
            return Ok(());
        }

        //Right offset always point to the beggining of a page
        let start_right_offset = self.k * self.sort_page_size_bytes;

        match self.file.get_next_page_offset(start_right_offset)? {
            Some(new_offset) => {
                self.state = SortFilePageIteratorState::LeftAndRightAvailable;
                self.current_offset_right = new_offset;
                Ok(())
            },
            None => {
                self.state = SortFilePageIteratorState::OnlyLeftAvailable;
                Ok(())
            }
        }
    }
}