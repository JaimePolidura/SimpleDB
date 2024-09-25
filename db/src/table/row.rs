use std::fmt;
use std::fmt::Formatter;
use crate::table::record::Record;
use crate::table::table::Table;
use bytes::Bytes;
use shared::{utils, ColumnId};
use std::sync::Arc;
use crate::table::column_type::ColumnType;
use crate::table::table_descriptor::ColumnDescriptor;

#[derive(Clone)]
pub struct Row {
    pub(crate) storage_engine_record: Record,
    pub(crate) key_bytes: Bytes,

    pub(crate) selection: Arc<Vec<ColumnId>>,
    pub(crate) table: Arc<Table>
}

impl Row {
    pub(crate) fn create(
        storage_engine_record: Record,
        selection: &Arc<Vec<ColumnId>>,
        table: &Arc<Table>,
        key_bytes: Bytes
    ) -> Row {
        Row {
            selection: selection.clone(),
            storage_engine_record,
            table: table.clone(),
            key_bytes
        }
    }

    pub fn get_primary_column_value(&self) -> &Bytes {
        &self.key_bytes
    }

    //Expect column_name to have been validated before calling this function
    pub fn get_column_value(&self, column_name: &str) -> Option<&Bytes> {
        let column_id = *self.table.columns_by_name.get(column_name)?
            .value();
        self.storage_engine_record.get_value(column_id)
    }

    pub(crate) fn get_column_desc(&self, column_name: &str) -> Option<ColumnDescriptor> {
        self.table.get_column_desc(column_name)
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut string = String::from("Row [");
        let n_columns = self.table.get_columns().len();
        let mut count = 0;

        for column in self.table.get_columns().values() {
            if let Some(column_value) = self.storage_engine_record.get_value(column.column_id) {
                string.push_str(&column.column_name);
                string.push_str("= ");
                string.push_str((match column.column_type {
                    ColumnType::I8 => utils::bytes_to_i8(column_value).to_string(),
                    ColumnType::U8 => utils::bytes_to_u8(column_value).to_string(),
                    ColumnType::I16 => utils::bytes_to_i16_le(column_value).to_string(),
                    ColumnType::U16 => utils::bytes_to_u16_le(column_value).to_string(),
                    ColumnType::U32 => utils::bytes_to_u32_le(column_value).to_string(),
                    ColumnType::I32 => utils::bytes_to_i32_le(column_value).to_string(),
                    ColumnType::U64 => utils::bytes_to_u64_le(column_value).to_string(),
                    ColumnType::I64 => utils::bytes_to_i64_le(column_value).to_string(),
                    ColumnType::F32 => utils::bytes_to_f32_le(column_value).to_string(),
                    ColumnType::F64 => utils::bytes_to_f64_le(column_value).to_string(),
                    ColumnType::Boolean => if column_value[0] == 0x00 { String::from("false") } else { String::from("true") },
                    ColumnType::Varchar => String::from_utf8(column_value.to_vec()).unwrap(),
                    ColumnType::Date => todo!(),
                    ColumnType::Blob => format!("Blob {} bytes long", column_value.len()),
                    ColumnType::Null => panic!("")
                }).as_str());

                if count < n_columns {
                    count += 1;
                    string.push_str(", ");
                }
            }
        }

        string.push_str("]");

        write!(f, "{}", string.as_str())
    }
}