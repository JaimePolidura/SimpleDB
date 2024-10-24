use crate::sql::execution::sort::sort_page::SortPage;
use shared::SimpleDbError::{CannotReadSortFile, CannotWriteSortFile};
use shared::{SimpleDbError, SimpleDbFile};

#[derive(Clone)]
pub struct SortFile {
    file: SimpleDbFile,
    sort_page_size_bytes: usize,
}

impl SortFile {
    pub fn create(
        file: SimpleDbFile,
        sort_page_size_bytes: usize,
    ) -> SortFile {
        SortFile { sort_page_size_bytes, file }
    }

    pub fn write(&mut self, page: SortPage) -> Result<(), SimpleDbError> {
        self.file.write(&page.serialize(self.sort_page_size_bytes))
            .map_err(|e| CannotWriteSortFile(e))
    }

    //Reads an entire row, even if it overflows one page,
    //It returns the row and the next row offset to the supplied offset
    pub fn read_row_bytes(
        &self,
        offset: usize
    ) -> Result<Option<(Vec<u8>, usize, usize)>, SimpleDbError> {
        if offset >= self.sort_page_size_bytes {
            return Ok(None);
        }

        let first_page = self.read_single_page(offset)?;

        if first_page.is_normal_page() {
            return Ok(Some((first_page.row_bytes(), first_page.get_nrows(), offset + self.sort_page_size_bytes)));
        } else {
            let (overflow_row, new_offset) = self.read_overflow_page(first_page, offset)?;
            Ok(Some((overflow_row, 1, new_offset)))
        }
    }

    //Given an offset into a sort page file, this function will return the next offset on the file which points
    //at the beginning of a page. If the initial_offset points at the beginning of a page, that offset will be returned.
    //This function will return None, if the offset is out of bounds of the file
    pub fn get_next_page_offset(&self, initial_offset: usize) -> Result<Option<usize>, SimpleDbError> {
        if initial_offset >= self.file.size() {
            return Ok(None);
        }
        if !self.read_single_page(initial_offset)?.is_overflow_page() {
            return Ok(Some(initial_offset))
        }

        let mut current_offset = initial_offset;

        //Read pages until we find a non overflow page
        loop {
            current_offset = current_offset + self.sort_page_size_bytes;
            let mut current_page = self.read_single_page(current_offset)?;

            if !current_page.is_overflow_page() {
                return Ok(Some(current_offset));
            }
        }
    }

    pub fn size(&self) -> usize {
        self.file.size()
    }

    fn read_overflow_page(
        &self,
        first_page: SortPage,
        first_page_offset: usize
    ) -> Result<(Vec<u8>, usize), SimpleDbError> {
        let mut current_offset = first_page_offset + self.sort_page_size_bytes;
        let mut row_bytes = first_page.row_bytes();

        loop {
            let current_page = self.read_single_page(current_offset)?;

            current_offset += self.sort_page_size_bytes;
            row_bytes.extend(current_page.row_bytes());

            if current_page.is_last_overflow_page() {
                row_bytes.extend(current_page.row_bytes());
                return Ok((row_bytes, current_offset))
            }
        }
    }

    fn read_single_page(&self, offset: usize) -> Result<SortPage, SimpleDbError> {
        let mut left_sort_page_bytes = self.file.read(offset, self.sort_page_size_bytes)
            .map_err(|e| CannotReadSortFile(e))?;
        Ok(SortPage::deserialize(&mut left_sort_page_bytes.as_slice()))
    }
}