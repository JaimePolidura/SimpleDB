use shared::connection::Connection;
use shared::logger::{logger, SimpleDbLayer};
use shared::SimpleDbError;
use shared::SimpleDbError::{InvalidRequestBinaryFormat};

pub enum Request {
    //Authentication, standalone, statement
    Statement(Authentication, bool, String), //Request Type ID: 1
    Close(Authentication), //Request Type ID: 2
    UseDatabase(Authentication, String), //Request Type ID: 3
}

pub struct Authentication {
    pub password: String
}

impl Request {
    pub fn deserialize_from_connection(connection: &mut Connection) -> Result<Request, SimpleDbError> {
        let authentication = Authentication::deserialize_from_connection(connection)?;

        match connection.read_u8()? {
            1 => {
                let is_standalone = connection.read_u8()? != 0x00;
                let statement_length = connection.read_u32()?;
                let statement_bytes = connection.read_n(statement_length as usize)?;
                let statement = String::from_utf8(statement_bytes)
                    .map_err(|_| InvalidRequestBinaryFormat)?;

                logger().debug(SimpleDbLayer::Server, &format!(
                    "Received statement request. ConnectionID: {} Password: {} Statement: {}",
                    connection.connection_id(), authentication.password, statement
                ));

                Ok(Request::Statement(authentication, is_standalone, statement))
            },
            2 => {
                logger().debug(SimpleDbLayer::Server, &format!("Received close request. ConnectionID: {}", connection.connection_id()));
                Ok(Request::Close(authentication))
            },
            3 => {
                let database_name_length = connection.read_u32()?;
                let database_name_bytes = connection.read_n(database_name_length as usize)?;
                let database_name_string = String::from_utf8(database_name_bytes)
                    .map_err(|_| InvalidRequestBinaryFormat)?;

                logger().debug(SimpleDbLayer::Server, &format!(
                    "Received use database request. Password: {} Database: {}",
                    authentication.password, database_name_string
                ));

                Ok(Request::UseDatabase(authentication, database_name_string))
            },
            _ => Err(InvalidRequestBinaryFormat)
        }
    }

    pub fn get_authentication(&self) -> &Authentication {
        match self {
            Request::Statement(authentication, _, _) => authentication,
            Request::Close(authentication) => authentication,
            Request::UseDatabase(authentication, _) => authentication
        }
    }
}

impl Authentication {
    pub fn deserialize_from_connection(connection: &mut Connection) -> Result<Authentication, SimpleDbError> {
        let password_length = connection.read_u32()?;
        let password_bytes = connection.read_n(password_length as usize)?;
        let password_string = String::from_utf8(password_bytes)
            .map_err(|_| InvalidRequestBinaryFormat)?;

        Ok(Authentication { password: password_string })
    }
}