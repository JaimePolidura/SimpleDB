use crate::table::record::Record;
use crate::table::schema::Schema;
use crate::value::{Type, Value};
use bytes::{BufMut, Bytes};
use shared::SimpleDbError::CannotDecodeColumn;
use shared::{utils, SimpleDbError};
use std::fmt;
use std::fmt::Formatter;

#[derive(Clone)]
pub struct Row {
    pub(crate) storage_engine_record: Record,
    pub(crate) key_bytes: Bytes,

    pub(crate) schema: Schema
}

impl Row {
    pub(crate) fn create(
        storage_engine_record: Record,
        key_bytes: Bytes,
        schema: Schema,
    ) -> Row {
        Row {
            storage_engine_record,
            key_bytes,
            schema,
        }
    }

    pub fn get_primary_column_value(&self) -> &Bytes {
        &self.key_bytes
    }

    //Expect column_name to have been validated before calling this function
    //If emtpy, value will contain Value::Null
    pub fn get_column_value(&self, column_name: &str) -> Result<Value, SimpleDbError> {
        let column_data = self.schema.get_column(column_name)
            .ok_or(SimpleDbError::ColumnNotFound(column_name.to_string()))?;

        match self.storage_engine_record.get_value(column_data.column_id) {
            Some(column_bytes) => Value::deserialize(column_bytes.clone(), column_data.column_type)
                .map_err(|_| CannotDecodeColumn(column_name.to_string(), self.get_primary_column_value().clone())),
            None => Ok(Value::Null)
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
            if let Some(column_value) = self.storage_engine_record.get_value(column.column_id) {
                string.push_str(&column.column_name);
                string.push_str(" = ");
                string.push_str((match column.column_type {
                    Type::I8 => utils::bytes_to_i8(column_value).to_string(),
                    Type::U8 => utils::bytes_to_u8(column_value).to_string(),
                    Type::I16 => utils::bytes_to_i16_le(column_value).to_string(),
                    Type::U16 => utils::bytes_to_u16_le(column_value).to_string(),
                    Type::U32 => utils::bytes_to_u32_le(column_value).to_string(),
                    Type::I32 => utils::bytes_to_i32_le(column_value).to_string(),
                    Type::U64 => utils::bytes_to_u64_le(column_value).to_string(),
                    Type::I64 => utils::bytes_to_i64_le(column_value).to_string(),
                    Type::F32 => format!("{:.2}", utils::bytes_to_f32_le(column_value)).to_string(),
                    Type::F64 => format!("{:.2}", utils::bytes_to_f64_le(column_value)).to_string(),
                    Type::Boolean => if column_value[0] == 0x00 { String::from("false") } else { String::from("true") },
                    Type::String => String::from_utf8(column_value.to_vec()).unwrap(),
                    Type::Date => todo!(),
                    Type::Blob => format!("Blob {} bytes long", column_value.len()),
                    Type::Null => panic!("")
                }).as_str());

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