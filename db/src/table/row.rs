use std::collections::HashSet;
use crate::table::record::Record;
use crate::table::schema::Schema;
use bytes::BufMut;
use shared::{ColumnId, SimpleDbError, Value};
use std::fmt;
use std::fmt::Formatter;
use crate::selection::Selection;

#[derive(Clone)]
pub struct Row {
    pub(crate) storage_engine_record: Record,
    pub(crate) primary_column_value: Value,

    pub(crate) schema: Schema
}

impl Row {
    pub(crate) fn create(
        storage_engine_record: Record,
        primary_column_value: Value,
        schema: Schema,
    ) -> Row {
        Row {
            storage_engine_record,
            primary_column_value,
            schema,
        }
    }

    pub fn project_selection(&mut self, selection: &Selection) {
        match selection {
            Selection::Some(_) => {
                self.storage_engine_record.project_selection(&selection.to_columns_id(&self.schema)
                    .unwrap().into_iter()
                    .collect());
            }
            Selection::All => {}
        }
    }

    pub fn get_primary_column_value(&self) -> &Value {
        &self.primary_column_value
    }

    //Expect column_name to have been validated before calling this function
    //If emtpy, value will contain Value::Null
    pub fn get_column_value(&self, column_name: &str) -> Result<Value, SimpleDbError> {
        let column_data = self.schema.get_column(column_name)
            .ok_or(SimpleDbError::ColumnNotFound(column_name.to_string()))?;

        match self.storage_engine_record.get_column_bytes(column_data.column_id) {
            Some(column_bytes) => Value::create(column_bytes.clone(), column_data.column_type),
            None => Ok(Value::create_null())
        }
    }

    pub fn serialize(self) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();
        serialized.put_u32_le(self.storage_engine_record.get_n_columns() as u32);
        serialized.extend(self.storage_engine_record.serialize());
        serialized
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut string = String::from("Row [");
        let columns = self.schema.get_columns();
        let n_columns = columns.len();
        let mut count = 0;

        let mut column_names: Vec<_> = columns.iter()
            .map(|column| column.column_name.clone())
            .collect();
        column_names.sort_by(|a, b| {
            (*a).cmp(b)
        });

        for column in column_names {
            let column = self.schema.get_column(&column).unwrap();
            if let Some(column_value_bytes) = self.storage_engine_record.get_column_bytes(column.column_id) {
                string.push_str(&column.column_name);
                string.push_str(" = ");

                string.push_str(&Value::create(column_value_bytes.clone(), column.column_type).unwrap()
                    .to_string());

                count += 1;
                if count < n_columns {
                    string.push_str(", ");
                }
            }
        }

        string.push_str("]");

        write!(f, "{}", string.as_str())
    }
}