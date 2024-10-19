#[derive(Debug, Clone, PartialEq)]
pub struct Sort {
    pub(crate) column_name: String,
    pub(crate) order: SortOrder
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc, Desc
}