use bytes::BufMut;
use db::{ColumnDescriptor, Row};
use shared::{ErrorTypeId, SimpleDbError};
use crate::server::ConnectionId;

pub enum Response {
    Statement(StatementResponse),
    Init(ConnectionId),
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
    columns_desc: Vec<ColumnDescriptor>,
    rows: Vec<Row>,
}

impl Response {
    pub fn from_simpledb_error(error: SimpleDbError) -> Response {
        Response::Error(error.serialize())
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();

        serialized.put_u8(self.message_type_id());
        serialized.extend(self.serialize_message_content());

        serialized
    }

    fn serialize_message_content(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        match self {
            Response::Statement(result) => serialized.extend(result.serialize()),
            Response::Init(connection_id) => serialized.put_u64_le((*connection_id) as u64),
            Response::Error(errorTypeId) => serialized.put_u8(*errorTypeId as u8),
            Response::Ok => {},
        };

        serialized
    }

    fn message_type_id(&self) -> u8 {
        match self {
            Response::Statement(_) => 1,
            Response::Init(_) => 2,
            Response::Error(_) => 3,
            Response::Ok => 4
        }
    }
}

impl QueryDataResponse {
    pub fn create(
        columns_desc: Vec<ColumnDescriptor>,
        rows: Vec<Row>
    ) -> QueryDataResponse {
        QueryDataResponse { columns_desc, rows }
    }
}

impl StatementResponse {
    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.put_u8(self.statement_response_type_id());

        match self {
            StatementResponse::Describe(columns_desc) => serialized.extend(Self::serialize_columns_desc(columns_desc)),
            StatementResponse::Databases(databases) => serialized.extend(Self::serialize_string_vec(databases)),
            StatementResponse::Tables(tables) => serialized.extend(Self::serialize_string_vec(tables)),
            StatementResponse::Data(data) => serialized.extend(Self::serialize_query_data(data)),
            StatementResponse::Ok(n_affected_rows) => serialized.put_u64_le(*n_affected_rows as u64),
        };

        serialized
    }

    fn serialize_query_data(
        query_data_response: &QueryDataResponse
    ) -> Vec<u8> {
        let mut serialized = Vec::new();

        //Columns desc
        serialized.extend(Self::serialize_columns_desc(&query_data_response.columns_desc));
        //Rows
        serialized.put_u32_le(query_data_response.rows.len() as u32);
        for row in &query_data_response.rows {
            serialized.extend(row.clone().serialize());
        }

        serialized
    }

    fn serialize_columns_desc(
        columns_desc: &Vec<ColumnDescriptor>
    ) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.put_u32_le(columns_desc.len() as u32);
        for columns_desc in columns_desc {
            serialized.extend(columns_desc.serialize());
        }

        serialized
    }

    fn serialize_string_vec(strings: &Vec<String>) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.put_u32_le(strings.len() as u32);
        for string in strings {
            serialized.put_u32_le(string.len() as u32);
            serialized.extend(string.bytes());
        }

        serialized
    }

    fn statement_response_type_id(&self) -> u8 {
        match self {
            StatementResponse::Ok(_) => 1,
            StatementResponse::Data(_) => 2,
            StatementResponse::Databases(_) => 3,
            StatementResponse::Tables(_) => 4,
            StatementResponse::Describe(_) => 5,
        }
    }
}