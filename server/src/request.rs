use shared::{SimpleDbError};
use shared::connection::Connection;
use shared::SimpleDbError::{CannotDecodeNetworkMessage, InvalidRequest};
use crate::server::ConnectionId;

pub enum Request {
    Statement(Authentication, ConnectionId, String), //Request Type ID: 1
    Close(Authentication, ConnectionId), //Request Type ID: 2
    Init(Authentication, String), //Request Type ID: 3
}

pub struct Authentication {
    pub password: String
}

impl Request {
    pub fn deserialize_from_connection(connection: &mut Connection) -> Result<Request, SimpleDbError> {
        let authentication = Authentication::deserialize_from_connection(connection)?;

        match connection.read_u8()? {
            1 => {
                let connection_id = connection.read_u64()? as ConnectionId;
                let statement_length = connection.read_u32()?;
                let statement_bytes = connection.read_n(statement_length as usize)?;
                let statement = String::from_utf8(statement_bytes)
                    .map_err(|_| InvalidRequest)?;
                Ok(Request::Statement(authentication, connection_id, statement))
            },
            2 => {
                let connection_id = connection.read_u64()? as ConnectionId;
                Ok(Request::Close(authentication, connection_id))
            },
            3 => {
                let database_name_length = connection.read_u32()?;
                let database_name_bytes = connection.read_n(database_name_length as usize)?;
                let database_name_string = String::from_utf8(database_name_bytes)
                    .map_err(|_| InvalidRequest)?;
                Ok(Request::Init(authentication, database_name_string))
            },
            _ => Err(SimpleDbError::InvalidRequest)
        }
    }

    pub fn get_authentication(&self) -> &Authentication {
        match self {
            Request::Statement(authentication, _, _) => authentication,
            Request::Close(authentication, _) => authentication,
            Request::Init(authentication, _) => authentication
        }
    }
}

impl Authentication {
    pub fn deserialize_from_connection(connection: &mut Connection) -> Result<Authentication, SimpleDbError> {
        let password_length = connection.read_u32()?;
        let password_bytes = connection.read_n(password_length as usize)?;
        let password_string = String::from_utf8(password_bytes)
            .map_err(|_| CannotDecodeNetworkMessage(String::from("Cannot decode password")))?;

        Ok(Authentication { password: password_string })
    }
}