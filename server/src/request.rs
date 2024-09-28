use crate::connection::Connection;
use shared::SimpleDbError;
use shared::SimpleDbError::InvalidRequest;
use crate::server::ConnectionId;

pub enum Request {
    Statement(ConnectionId, String), //Request Type ID: 1
    Close(ConnectionId), //Request Type ID: 2
    Init(String), //Request Type ID: 3
}

impl Request {
    pub fn deserialize_from_connection(connection: &mut Connection) -> Result<Request, SimpleDbError> {
        match connection.read_u8()? {
            1 => {
                let connection_id = connection.read_u64()? as ConnectionId;
                let statement_length = connection.read_u32()?;
                let statement_bytes = connection.read_n(statement_length as usize)?;
                let statement = String::from_utf8(statement_bytes)
                    .map_err(|_| InvalidRequest)?;
                Ok(Request::Statement(connection_id, statement))
            },
            2 => {
                let connection_id = connection.read_u64()? as ConnectionId;
                Ok(Request::Close(connection_id))
            },
            3 => {
                let database_name_length = connection.read_u32()?;
                let database_name_bytes = connection.read_n(database_name_length as usize)?;
                let database_name_string = String::from_utf8(database_name_bytes)
                    .map_err(|_| InvalidRequest)?;
                Ok(Request::Init(database_name_string))
            },
            _ => Err(SimpleDbError::InvalidRequest)
        }
    }
}