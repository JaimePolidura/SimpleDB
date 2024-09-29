use std::net::TcpStream;
use shared::connection::Connection;
use crate::request::Request;
use crate::response::Response;

pub struct SimpleDbServer {
    connection: Connection,
    password: String,
}

impl SimpleDbServer {
    pub fn send_request(&mut self, request: Request) -> Response {
        let serialized = request.serialize();
        self.connection.write(serialized).expect("Cannto write to server");
        Response::deserialize_from_connection(&mut self.connection)
    }

    pub fn create(
        address: String,
        password: String,
    ) -> SimpleDbServer {
        match TcpStream::connect(address) {
            Ok(stream) => {
                SimpleDbServer {
                    connection: Connection::create(stream),
                    password
                }
            },
            Err(e) => panic!("{}", e)
        }
    }
}
