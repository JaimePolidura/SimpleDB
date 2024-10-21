use crate::selection::Selection;
use crate::table::record::{Record, RecordBuilder};
use crate::table::schema::Schema;
use bytes::{BufMut, Bytes};
use shared::{SimpleDbError, Value};
pub use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;

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

    pub fn deserialize(bytes: Vec<u8>, schema: &Schema) -> Row {
        let primary_column = schema.get_primary_column();
        let record = Record::deserialize(bytes);
        let primary_column_value = Value::create(
            record.get_column_bytes(primary_column.column_id).unwrap().clone(),
            primary_column.column_type
        ).unwrap();

        Row {
            storage_engine_record: record,
            schema: schema.clone(),
            primary_column_value
        }
    }

    pub fn serialize(self) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();
        serialized.put_u32_le(self.storage_engine_record.get_n_columns() as u32);
        serialized.extend(self.storage_engine_record.serialize());
        serialized
    }

    pub fn serialized_size(&self) -> usize {
        self.storage_engine_record.serialize_size() + 4
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

pub struct RowBuilder {
    pub(crate) storage_record_builder: RecordBuilder,
    pub(crate) schema: Schema,
    pub(crate) primary_value: Option<Value>,
}

impl RowBuilder {
    pub fn create(schema: Schema) -> RowBuilder {
        RowBuilder {
            storage_record_builder: Record::builder(),
            primary_value: None,
            schema,
        }
    }

    pub fn add_by_column_name(&mut self, value: Bytes, column_name: &str) {
        let column = self.schema.get_column(column_name).unwrap();
        self.storage_record_builder.add_column(column.column_id, value);
    }

    pub fn add_primary_value(&mut self, value: Value) {
        let primary_value_column = self.schema.get_primary_column();
        self.storage_record_builder.add_column(primary_value_column.column_id, value.get_bytes().clone());
        self.primary_value = Some(value);
    }

    pub fn build(self) -> Row {
        Row {
            storage_engine_record: self.storage_record_builder.build(),
            primary_column_value: self.primary_value.unwrap(),
            schema: self.schema
        }
    }
}