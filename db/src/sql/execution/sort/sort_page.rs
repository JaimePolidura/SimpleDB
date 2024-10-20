use shared::Flag;
use crate::Row;

const SORT_PAGE_NORMAL_PAGE: usize = 1 << 0;
const SORT_PAGE_NEXT_PAGE_OVERFLOW: usize = 1 << 2;
const SORT_PAGE_OVERFLOW_PAGE: usize = 1 << 1;

pub struct SortPage {
    flags: Flag,
    n_rows: usize,
    serialized_row: Vec<Row>
}