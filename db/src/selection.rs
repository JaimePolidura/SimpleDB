use crate::table::schema::Schema;
use shared::{ColumnId, SimpleDbError};
use std::collections::HashSet;

#[derive(Clone)]
pub enum Selection {
    All,
    Some(Vec<String>)
}

//Describes the type of columns present in the selection.
#[derive(Clone)]
pub enum IndexSelectionType {
    Primary, //Only primary has been selected.
    Secondary, //Only secondary has been selected.
    OnlyPrimaryAndSecondary, //Only primary and one secondary column have been selected
    All //Other rows have been selected
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

    pub fn get_index_selection_type(&self, schema: &Schema) -> IndexSelectionType {
        match &self {
            Selection::Some(selected_columns) => {
                let mut one_secondary_indexed_column_selected = false;
                let mut primary_column_selected = false;

                for selected_column_name in selected_columns {
                    let column = schema.get_column(selected_column_name).unwrap();
                    if column.is_primary {
                        primary_column_selected = true;
                    } else if column.is_secondary_indexed() && !one_secondary_indexed_column_selected {
                        one_secondary_indexed_column_selected = true;
                    } else {
                        return IndexSelectionType::All;
                    }
                }

                if one_secondary_indexed_column_selected && primary_column_selected {
                    IndexSelectionType::OnlyPrimaryAndSecondary
                } else if one_secondary_indexed_column_selected {
                    IndexSelectionType::Secondary
                } else if primary_column_selected {
                    IndexSelectionType::Primary
                } else {
                    return IndexSelectionType::All;
                }
            }
            Selection::All => IndexSelectionType::All,
        }
    }
}