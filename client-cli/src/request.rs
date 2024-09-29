use bytes::BufMut;

pub enum Request {
    //Password, connectionId, statement
    Statement(String, usize, String), //Request Type ID: 1
    //Password, connectionId
    Close(String, usize), //Request Type ID: 2
    //Password, database
    InitConnection(String, String), //Request Type ID: 3
}

impl Request {
    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.extend(self.serialize_auth());

        match self {
            Request::Statement(_, connectionId, statement) => {
                serialized.put_u8(1);
                serialized.put_u64_le(*connectionId as u64);
                serialized.put_u32_le(statement.len() as u32);
                serialized.extend(statement.bytes());
            },
            Request::Close(_, connectionId) => {
                serialized.put_u8(1);
                serialized.put_u64_le(*connectionId as u64);
            },
            Request::InitConnection(_, database_name) => {
                serialized.put_u8(3);
                serialized.put_u32_le(database_name.len() as u32);
                serialized.extend(database_name.bytes());
            }
        };

        serialized
    }

    fn serialize_auth(&self) -> Vec<u8>  {
        let mut serialized: Vec<u8> = Vec::new();
        let password = self.get_password();
        serialized.put_u32_le(password.len() as u32);
        serialized.extend(password.bytes());
        serialized
    }

    pub fn get_password(&self) -> &String {
        match self {
            Request::Statement(password, _, _) => password,
            Request::Close(password, _) => password,
            Request::InitConnection(password, _) => password,
        }
    }
}