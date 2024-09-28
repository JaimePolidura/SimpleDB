use shared::SimpleDbError;

pub enum Response {
    Init(usize),
    StatementResult(),
    Error(usize) //Error number
}

impl Response {
    pub fn from_simpledb_error(error: SimpleDbError) -> Response {
        todo!()
    }

    pub fn serialize(&self) -> Vec<u8> {
        todo!()
    }
}