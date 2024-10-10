use bytes::BufMut;
use db::{ColumnDescriptor, IndexType, Row};
use shared::{ErrorTypeId, SimpleDbError};

pub enum Response {
    Statement(StatementResponse),
    Error(ErrorTypeId), //Error number
    Ok,
}

pub enum StatementResponse {
    Ok(usize),
    Data(QueryDataResponse),
    Databases(Vec<String>),
    Tables(Vec<String>),
    Indexes(Vec<(String, IndexType)>),
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
            Response::Error(error_type_id) => serialized.put_u8(*error_type_id as u8),
            Response::Ok => {},
        };

        serialized
    }

    fn message_type_id(&self) -> u8 {
        match self {
            Response::Statement(_) => 1,
            Response::Error(_) => 2,
            Response::Ok => 3
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
            StatementResponse::Indexes(indexes) => serialized.extend(Self::serialize_show_indexes(indexes)),
            StatementResponse::Data(data) => serialized.extend(Self::serialize_query_data(data)),
            StatementResponse::Tables(tables) => serialized.extend(Self::serialize_string_vec(tables)),
            StatementResponse::Ok(n_affected_rows) => serialized.put_u64_le(*n_affected_rows as u64),
        };

        serialized
    }

    fn serialize_show_indexes(
        indexes: &Vec<(String, IndexType)>
    ) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();

        serialized.put_u32_le(indexes.len() as u32);
        for (indexed_column_name, index_type) in indexes {
            serialized.put_u32_le(indexed_column_name.len() as u32);
            serialized.extend(indexed_column_name.as_bytes());
            serialized.put_u8(index_type.serialize());
        }

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
            StatementResponse::Indexes(_) => 6,
        }
    }
}