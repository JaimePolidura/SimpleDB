use crate::connection::Connection;
use shared::SimpleDbError;

pub enum Request {
    Statement,
    Close,
    Init,
}

impl Request {
    pub fn deserialize_from_connection(other: &mut Connection) -> Result<Request, SimpleDbError> {
        todo!()
    }
}