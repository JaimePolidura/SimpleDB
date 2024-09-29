use std::collections::HashMap;
use std::fmt;
use bytes::Bytes;
use shared::{utils, ColumnId, ErrorTypeId};
use shared::connection::Connection;

pub enum Response {
    Statement(StatementResponse),
    Init(usize), //ConnectionId
    Error(ErrorTypeId), //Error number
    Ok,
}

pub enum StatementResponse {
    Ok(usize),
    Data(QueryDataResponse),
    Databases(Vec<String>),
    Tables(Vec<String>),
    Describe(Vec<ColumnDescriptor>)
}

pub struct QueryDataResponse {
    pub columns_desc: Vec<ColumnDescriptor>,
    pub rows: Vec<Row>,
}

pub struct Row {
    pub columns: HashMap<ColumnId, Bytes>,
}

pub struct ColumnDescriptor {
    pub column_id: ColumnId,
    pub column_type: ColumnType,
    pub column_name: String,
    pub is_primary: bool,
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
    Null
}

impl Response {
    pub fn deserialize_from_connection(connection: &mut Connection) -> Response {
        match connection.read_u8().expect("Cannot read response type ID") {
            1 => {
                Response::Statement(match connection.read_u8().expect("Cannot read response statement type ID") {
                    1 => StatementResponse::Ok(connection.read_u64().expect("Cannot read Nº Affected rows") as usize),
                    2 => StatementResponse::Data(Self::deserialize_query_response(connection)),
                    3 => StatementResponse::Databases(Self::deserialize_string_vec(connection)),
                    4 => StatementResponse::Tables(Self::deserialize_string_vec(connection)),
                    5 => StatementResponse::Describe(Self::deserialize_column_dec(connection)),
                    _ => panic!("Invalid statement response type Id")
                })
            },
            2 => Response::Init(connection.read_u64().expect("Cannot read connection ID") as usize),
            3 => Response::Error(connection.read_u8().expect("Cannot read response error type ID")),
            4 => Response::Ok,
            _ => panic!("Invalid server response type Id")
        }
    }

    fn deserialize_column_dec(connection: &mut Connection) -> Vec<ColumnDescriptor> {
        let n_items = connection.read_u32().expect("Cannot read columns desc Nº entries");
        let mut vec: Vec<ColumnDescriptor> = Vec::with_capacity(n_items as usize);

        for _ in 0..n_items {
            let column_id = connection.read_u16().expect("Cannot read columns ID");
            let column_type = connection.read_u8().expect("Cannot read column type");
            let is_primary = connection.read_u8().expect("Cannot read is primary") != 0;
            let column_name_length = connection.read_u32().expect("Cannot read column value length");
            let column_name_bytes = connection.read_n(column_name_length as usize).expect("Cannot read column value bytes");
            let column_name_string = String::from_utf8(column_name_bytes)
                .expect("Cannot convert column name to UTF-8 string");

            vec.push(ColumnDescriptor {
                column_type: ColumnType::deserialize(column_type),
                column_name: column_name_string,
                column_id,
                is_primary
            });
        }

        vec
    }

    fn deserialize_query_response(connection: &mut Connection) -> QueryDataResponse {
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

        QueryDataResponse {
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
            ColumnType::Null => panic!("")
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
            ColumnType::Null => "Null",
        }
    }
}