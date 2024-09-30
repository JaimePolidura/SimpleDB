use std::net::TcpStream;
use shared::connection::Connection;
use crate::request::Request;
use crate::response::Response;

pub struct SimpleDbServer {
    connection: Connection,
}

impl SimpleDbServer {
    pub fn create(
        address: String,
    ) -> SimpleDbServer {
        println!("simpledb> Connecting to {}!", address);

        match TcpStream::connect(address.clone()) {
            Ok(stream) => {
                println!("simpledb> Connected to {}!", address);
                SimpleDbServer { connection: Connection::create(stream), }
            },
            Err(_) => panic!("ERROR Cannot connect to {}. Make sure the server is running or the address is correct", address)
        }
    }

    pub fn send_request(&mut self, request: Request) -> Response {
        let serialized = request.serialize();
        self.connection.write(serialized).expect("Cannto write to server");
        Response::deserialize_from_connection(&mut self.connection)
    }
}
