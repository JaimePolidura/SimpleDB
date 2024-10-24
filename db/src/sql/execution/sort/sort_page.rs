use bytes::{Buf, BufMut};
use shared::{utils, Flag, FlagMethods};

pub const SORT_PAGE_FIRST_PAGE_OVERFLOW: Flag = 1 << 3;
pub const SORT_PAGE_OVERFLOW_PAGE: Flag = 1 << 2;
pub const SORT_PAGE_LAST_PAGE_OVERFLOW: Flag = 1 << 1;
pub const SORT_PAGE_NORMAL_PAGE: Flag = 1 << 0;

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

    pub fn create_first_page_overflow(
        row_bytes: Vec<u8>,
        n_rows: usize
    ) -> SortPage {
        SortPage { flags: SORT_PAGE_FIRST_PAGE_OVERFLOW as Flag, row_bytes, n_rows, }
    }

    pub fn create_last_page_overflow(
        row_bytes: Vec<u8>,
        n_rows: usize
    ) -> SortPage {
        SortPage { flags: SORT_PAGE_LAST_PAGE_OVERFLOW as Flag, row_bytes, n_rows, }
    }

    pub fn get_nrows(&self) -> usize {
        self.n_rows
    }

    pub fn deserialize(bytes: &mut &[u8]) -> SortPage {
        let n_rows = bytes.get_u32_le() as usize;
        let flags = bytes.get_u64_le() as Flag;
        let row_bytes = bytes.to_vec();

        SortPage { row_bytes, n_rows, flags, }
    }

    pub fn serialize(&self, page_size: usize) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();

        serialized.put_u32_le(self.n_rows as u32);
        serialized.put_u64_le(self.flags);
        serialized.extend(&self.row_bytes);

        //Fill remaining bytes to 0
        let remaining_bytes_to_fill = page_size - serialized.len();
        utils::fill_vec(&mut serialized, remaining_bytes_to_fill, 0);

        serialized
    }

    pub fn is_last_overflow_page(&self) -> bool {
        self.flags.has(SORT_PAGE_LAST_PAGE_OVERFLOW)
    }

    pub fn is_first_overflow_page(&self) -> bool {
        self.flags.has(SORT_PAGE_FIRST_PAGE_OVERFLOW)
    }

    pub fn is_normal_page(&self) -> bool {
        self.flags.has(SORT_PAGE_NORMAL_PAGE)
    }

    pub fn row_bytes(&self) -> Vec<u8> {
        self.row_bytes.clone()
    }

    pub fn is_overflow_page(&self) -> bool {
        self.flags.has(SORT_PAGE_OVERFLOW_PAGE)
    }

    pub fn header_size_bytes() -> usize {
        4 + 8
    }
}