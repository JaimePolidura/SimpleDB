use crate::Schema;

#[derive(Debug, Clone, PartialEq)]
pub struct Sort {
    pub(crate) column_name: String,
    pub(crate) order: SortOrder
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc, Desc
}

impl Sort {
    pub fn is_indexed(&self, schema: Schema) -> bool {
        let column = schema.get_column(&self.column_name).unwrap();
        column.is_secondary_indexed() || column.is_primary
    }
}