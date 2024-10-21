use bytes::BufMut;
use shared::{utils, Flag};

pub const SORT_PAGE_NORMAL_PAGE: usize = 1 << 0;
pub const SORT_PAGE_LAST_PAGE_OVERFLOW: usize = 1 << 2;
pub const SORT_PAGE_OVERFLOW_PAGE: usize = 1 << 1;

pub struct SortPage {
    flags: Flag,
    n_rows: usize,
    row_bytes: Vec<u8>
}

impl SortPage {
    pub fn create_normal(
        row_bytes: Vec<u8>,
        n_rows: usize
    ) -> SortPage {
        SortPage { flags: SORT_PAGE_NORMAL_PAGE as Flag, row_bytes, n_rows, }
    }

    pub fn create_next_page_overflow(
        row_bytes: Vec<u8>,
        n_rows: usize
    ) -> SortPage {
        SortPage { flags: SORT_PAGE_OVERFLOW_PAGE as Flag, row_bytes, n_rows, }
    }

    pub fn create_last_page_overflow(
        row_bytes: Vec<u8>,
        n_rows: usize
    ) -> SortPage {
        SortPage { flags: SORT_PAGE_LAST_PAGE_OVERFLOW as Flag, row_bytes, n_rows, }
    }

    pub fn serialize(&self, expected_serialized_size: usize) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();

        serialized.put_u32_le(self.n_rows as u32);
        serialized.put_u64_le(self.flags);
        serialized.extend(&self.row_bytes);

        //Fill remaining bytes to 0
        let remaining_bytes_to_fill = expected_serialized_size - serialized.len();
        utils::fill_vec(&mut serialized, remaining_bytes_to_fill, 0);

        serialized
    }

    pub fn header_size_bytes() -> usize {
        4 + 8
    }
}