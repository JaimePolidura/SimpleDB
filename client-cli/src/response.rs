use bytes::Bytes;
use shared::connection::Connection;
use shared::{utils, ColumnId, ErrorTypeId};
use std::collections::HashMap;

pub enum IndexType {
    Primary,
    Secondary,
}

pub enum Response {
    Statement(StatementResponse),
    Error(ErrorTypeId, String), //Error number, error message
    Ok,
}

pub enum StatementResponse {
    Ok(usize),
    Rows(RowsResponse),
    Databases(Vec<String>),
    Tables(Vec<String>),
    Describe(Vec<Column>),
    Indexes(Vec<(String, IndexType)>),
    Explain(Vec<String>)
}

pub struct RowsResponse {
    pub columns_desc: Vec<Column>,
    pub rows: Vec<Row>,
}

pub struct Row {
    pub columns: HashMap<ColumnId, Bytes>,
}

pub struct Column {
    pub column_id: ColumnId,
    pub column_type: ColumnType,
    pub column_name: String,
    pub is_primary: bool,
    pub is_indexed: bool,
}

pub enum ColumnType {
    I8,
    U8,
    I16,
    U16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    Boolean,
    String,
    Date,
    Blob,
}

impl Response {
    pub fn deserialize_from_connection(connection: &mut Connection) -> Response {
        match connection.read_u8().unwrap() {
            1 => {
                Response::Statement(match connection.read_u8().expect("Cannot read response statement type ID") {
                    1 => StatementResponse::Ok(connection.read_u64().expect("Cannot read Nº Affected rows") as usize),
                    2 => StatementResponse::Rows(Self::deserialize_query_response(connection)),
                    3 => StatementResponse::Databases(Self::deserialize_string_vec(connection)),
                    4 => StatementResponse::Tables(Self::deserialize_string_vec(connection)),
                    5 => StatementResponse::Describe(Self::deserialize_column_dec(connection)),
                    6 => StatementResponse::Indexes(Self::deserialize_indexes(connection)),
                    7 => StatementResponse::Explain(Self::deserialize_explain(connection)),
                    _ => panic!("Invalid statement response type Id")
                })
            },
            2 => Response::Error(connection.read_u8().expect("Cannot read response error type ID"), Self::deserialize_error_message(connection)),
            3 => Response::Ok,
            _ => panic!("Invalid server response type Id")
        }
    }

    fn deserialize_error_message(connection: &mut Connection) -> String {
        let error_message_length = connection.read_u32().expect("Cannot read error message length");
        let error_message_bytes = connection.read_n(error_message_length as usize).expect("Cannot read error message bytes");

        String::from_utf8(error_message_bytes)
            .expect("Cannot convert error message bytes to UTF-8 String")
    }

    fn deserialize_explain(connection: &mut Connection) -> Vec<String> {
        let n_lines = connection.read_u32().expect("Cannot read n lines");
        let mut lines = Vec::new();

        for _ in 0..n_lines {
            let line_length = connection.read_u32().expect("Cannot read line length");
            let line_bytes = connection.read_n(line_length as usize).expect("Cannot read line bytes");
            lines.push(String::from_utf8(line_bytes).expect("Cannot convert line to UTF-8 string"));
        }

        lines
    }

    fn deserialize_indexes(connection: &mut Connection) -> Vec<(String, IndexType)> {
        let n_indexes = connection.read_u32().expect("Cannot read Nº Indexes");
        let mut indexes = Vec::new();

        for _ in 0..n_indexes {
            let column_name_length = connection.read_u32().expect("Cannot read column name length");
            let column_name_bytes = connection.read_n(column_name_length as usize)
                .expect("Cannot read column name bytes");
            let column_name = String::from_utf8(column_name_bytes)
                .expect("Cannot read column name as UTF-8 String");
            let index_type = match connection.read_u8().expect("Cannot read index type") {
                1 => IndexType::Primary,
                2 => IndexType::Secondary,
                other => panic!("{}", format!("Unknown index type id {}", other))
            };

            indexes.push((column_name, index_type));
        }

        indexes
    }

    fn deserialize_column_dec(connection: &mut Connection) -> Vec<Column> {
        let n_items = connection.read_u32().expect("Cannot read columns desc Nº entries");
        let mut vec: Vec<Column> = Vec::with_capacity(n_items as usize);

        for _ in 0..n_items {
            let column_id = connection.read_u16().expect("Cannot read columns ID");
            let column_type = connection.read_u8().expect("Cannot read column type");
            let is_primary = connection.read_u8().expect("Cannot read is primary") != 0;
            let is_indexed = connection.read_u64().expect("Cannot read is indexed") != 0xFFFFFFFFFFFFFFFF;
            let column_name_length = connection.read_u32().expect("Cannot read column value length");
            let column_name_bytes = connection.read_n(column_name_length as usize).expect("Cannot read column value bytes");
            let column_name_string = String::from_utf8(column_name_bytes)
                .expect("Cannot convert column name to UTF-8 string");

            vec.push(Column {
                column_type: ColumnType::deserialize(column_type),
                column_name: column_name_string,
                is_indexed,
                is_primary,
                column_id,
            });
        }

        vec
    }

    fn deserialize_query_response(connection: &mut Connection) -> RowsResponse {
        let columns_desc = Self::deserialize_column_dec(connection);
        let n_rows = connection.read_u32().expect("Cannot read Nº rows");
        let mut rows = Vec::with_capacity(n_rows as usize);

        for _ in 0..n_rows {
            let n_columns = connection.read_u32().expect("Cannto read Nº Columns");
            let mut columns = HashMap::new();

            for _ in 0..n_columns {
                let column_id = connection.read_u16().expect("Cannot read column ID");
                let column_value_length = connection.read_u32().expect("Cannot read column value length");
                let column_value_bytes = connection.read_n(column_value_length as usize)
                    .expect("Cannot read column value bytes");

                columns.insert(column_id, Bytes::from(column_value_bytes));
            }

            rows.push(Row { columns });
        }

        RowsResponse {
            columns_desc,
            rows
        }
    }

    fn deserialize_string_vec(connection: &mut Connection) -> Vec<String> {
        let n_items = connection.read_u32().expect("Cannot read vec Nº entries");
        let mut vec: Vec<String> = Vec::with_capacity(n_items as usize);

        for _ in 0..n_items {
            let string_length = connection.read_u32().expect("Cannot read string length");
            let string_bytes = connection.read_n(string_length as usize).expect("Cannot read string");
            vec.push(String::from_utf8(string_bytes)
                .expect("Cannot read string"));
        }

        vec
    }
}

impl ColumnType {
    pub fn deserialize(column_type_id : u8) -> ColumnType {
        match column_type_id {
            1 =>  ColumnType::I8,
            2 =>  ColumnType::U8,
            3 =>  ColumnType::I16,
            4 =>  ColumnType::U16,
            5 =>  ColumnType::U32,
            6 =>  ColumnType::I32,
            7 =>  ColumnType::U64,
            8 =>  ColumnType::I64,
            9 =>  ColumnType::F32,
            10 => ColumnType::F64 ,
            11 => ColumnType::Boolean,
            12 => ColumnType::String,
            13 => ColumnType::Date,
            14 => ColumnType::Blob,
            _ => panic!("Cannot deserialize column type ID")
        }
    }

    pub fn bytes_to_string(&self, value: &Bytes) -> String {
        match self {
            ColumnType::I8 => utils::bytes_to_i8(value).to_string(),
            ColumnType::U8 => utils::bytes_to_u8(value).to_string(),
            ColumnType::I16 => utils::bytes_to_i16_le(value).to_string(),
            ColumnType::U16 => utils::bytes_to_u16_le(value).to_string(),
            ColumnType::U32 => utils::bytes_to_u32_le(value).to_string(),
            ColumnType::I32 => utils::bytes_to_i32_le(value).to_string(),
            ColumnType::U64 => utils::bytes_to_u64_le(value).to_string(),
            ColumnType::I64 => utils::bytes_to_i64_le(value).to_string(),
            ColumnType::F32 => format!("{:.2}", utils::bytes_to_f32_le(value)).to_string(),
            ColumnType::F64 => format!("{:.2}", utils::bytes_to_f64_le(value)).to_string(),
            ColumnType::Boolean => if value[0] == 0x00 { String::from("false") } else { String::from("true") }
            ColumnType::String => String::from_utf8(value.to_vec()).unwrap(),
            ColumnType::Date => todo!(),
            ColumnType::Blob => format!("{:02X?}", value.to_vec()),
        }
    }

    pub fn to_string(&self) -> &str {
        match self {
            ColumnType::I8 => "I8",
            ColumnType::U8 => "U8",
            ColumnType::I16 => "I16",
            ColumnType::U16 => "U16",
            ColumnType::U32 => "U32",
            ColumnType::I32 => "I32",
            ColumnType::U64 => "U64",
            ColumnType::I64 => "I64",
            ColumnType::F32 => "F32",
            ColumnType::F64 => "F64",
            ColumnType::Boolean => "Boolean",
            ColumnType::String => "String",
            ColumnType::Date => "Date",
            ColumnType::Blob => "Blob",
        }
    }
}