use crate::sql::execution::sort::sort_file::SortFile;
use crate::sql::execution::sort::sort_page_run_iterator::SortFilePageIteratorState::LeftAndRightAvailable;
use crate::{Row, Schema};
use shared::SimpleDbError;

//This iterator will maintain two pointers on the file (left and right) that will be separated by k
//When the pages returned by these pointers reaches n_pages_per_run, they (left & right( will be advanced by n_pages_per_run pages.
//This will be used by external merge sort algoritm (sorter.rs).
//'a lives as long as SortFiles lives
//k indicates the difference in sort pages that there will be in left & right offsets
//https://www.youtube.com/watch?v=F9XmmS8rL4c&t=698s
pub struct SortPageRunIterator {
    table_schema: Schema,

    sort_page_size_bytes: usize,
    file: SortFile,
    n_pages_to_return_per_run: usize,

    state: SortFilePageIteratorState,
    current_offset_left: usize,
    current_offset_right: usize,
    n_pages_returned_right_in_run: usize,
    n_pages_returned_left_in_run: usize,
    n_current_run: usize,
}

enum SortFilePageIteratorState {
    LeftAndRightAvailable,
    OnlyLeftAvailable
}

impl SortPageRunIterator {
    pub fn create(
        file: SortFile,
        sort_page_size_bytes: usize,
        schema: &Schema,
        pages_per_run: usize,
    ) -> Result<SortPageRunIterator, SimpleDbError> {
        let mut iterator = SortPageRunIterator {
            state: LeftAndRightAvailable,
            table_schema: schema.clone(),
            n_pages_returned_right_in_run: 0,
            n_pages_returned_left_in_run: 0,
            current_offset_right: 0, //Will get init when calling next_right()
            current_offset_left: 0,
            sort_page_size_bytes,
            n_pages_to_return_per_run: pages_per_run,
            n_current_run: 0,
            file,
        };

        iterator.initialize()?;

        Ok(iterator)
    }

    pub fn next_left(&mut self) -> Result<Option<Vec<Row>>, SimpleDbError> {
        self.maybe_go_to_next_run()?;

        //The user should call next_right
        if self.n_pages_returned_left_in_run == self.n_pages_to_return_per_run {
            return Ok(Some(Vec::new()));
        }

        match self.read_row(self.current_offset_left)? {
            Some((row, new_offset)) => {
                self.current_offset_left = new_offset;
                self.n_pages_returned_left_in_run += 1;
                Ok(Some(row))
            }
            None => Ok(None)
        }
    }

    //Expect next_left() and next_right() to be called at the same time
    pub fn next_right(&mut self) -> Result<Option<Vec<Row>>, SimpleDbError> {
        self.maybe_go_to_next_run()?;
        self.maybe_initialize_right_offset()?;

        //The user should call next_left
        if self.n_pages_returned_right_in_run == self.n_pages_to_return_per_run {
            return Ok(Some(Vec::new()));
        }

        match self.state {
            SortFilePageIteratorState::LeftAndRightAvailable => {
                match self.read_row(self.current_offset_right)? {
                    Some((row, new_offset)) => {
                        self.current_offset_right = new_offset;
                        self.n_pages_returned_right_in_run += 1;
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
        match self.state {
            SortFilePageIteratorState::LeftAndRightAvailable => {
                self.current_offset_left < self.file.size() || self.current_offset_right < self.file.size()
            },
            SortFilePageIteratorState::OnlyLeftAvailable => {
                self.current_offset_left < self.file.size()
            }
        }
    }

    fn maybe_go_to_next_run(&mut self) -> Result<(), SimpleDbError> {
        if self.n_pages_returned_right_in_run == self.n_pages_to_return_per_run &&
            self.n_pages_returned_left_in_run == self.n_pages_to_return_per_run &&
            matches!(self.state, SortFilePageIteratorState::LeftAndRightAvailable) {

            self.current_offset_left = self.current_offset_right;
            self.n_pages_returned_right_in_run = 0;
            self.n_pages_returned_left_in_run = 0;

            match self.file.get_next_page_offset(
                self.current_offset_right + (self.sort_page_size_bytes * self.n_pages_to_return_per_run)
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
        match self.file.get_next_page_offset(self.current_offset_left * self.n_pages_to_return_per_run)? {
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
        if self.n_pages_to_return_per_run * self.sort_page_size_bytes >= self.file.size() {
            self.state = SortFilePageIteratorState::OnlyLeftAvailable;
            return Ok(());
        }

        //Right offset always point to the beggining of a page
        let start_right_offset = self.n_pages_to_return_per_run * self.sort_page_size_bytes;

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