use crate::table::schema::Schema;
use shared::{ColumnId, SimpleDbError};

#[derive(Clone)]
pub enum Selection {
    All,
    Some(Vec<String>)
}

impl Selection {
    pub fn is_empty(&self) -> bool {
        match self {
            Selection::Some(list) => list.is_empty(),
            Selection::All => false,
        }
    }

    pub fn get_some_selected_columns(&self) -> Vec<String> {
        match &self {
            Selection::Some(values) => values.clone(),
            Selection::All => Vec::new(),
        }
    }

    pub fn to_columns_id(
        &self,
        schema: &Schema,
    ) -> Result<Vec<ColumnId>, SimpleDbError> {
        match &self {
            Selection::Some(columns_names) => {
                let mut column_ids = Vec::new();

                for column_name in columns_names {
                    let column = schema.get_column_or_err(column_name)?;
                    column_ids.push(column.column_id);
                }

                Ok(column_ids)
            },
            Selection::All => {
                Ok(schema.get_columns().iter()
                    .map(|column| column.column_id)
                    .collect())
            }
        }
    }
}