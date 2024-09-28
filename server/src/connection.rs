use std::io::{Read, Write};
use shared::SimpleDbError;
use shared::SimpleDbError::CannotDecodeNetworkMessage;
use std::net::TcpStream;

pub struct Connection {
    tcp_stream: TcpStream
}

impl Connection {
    pub fn create(mut tcp_stream: TcpStream) -> Connection {
        Connection { tcp_stream }
    }

    pub fn read_u8(&mut self) -> Result<u8, SimpleDbError> {
        let mut buf = [0u8; 1];
        self.tcp_stream.read_exact(&mut buf)
            .map_err(|e| CannotDecodeNetworkMessage(String::from("Cannot read message U8")))?;
        Ok(buf[0])
    }

    pub fn read_u16(&mut self) -> Result<u16, SimpleDbError> {
        let mut buff = [0u8; 2];
        self.tcp_stream.read_exact(&mut buff)
            .map_err(|e| CannotDecodeNetworkMessage(String::from("Cannot read message U16")))?;
        Ok(u16::from_le_bytes(buff))
    }

    pub fn read_u32(&mut self) -> Result<u32, SimpleDbError> {
        let mut buff = [0u8; 4];
        self.tcp_stream.read_exact(&mut buff)
            .map_err(|e| CannotDecodeNetworkMessage(String::from("Cannot read message U32")))?;
        Ok(u32::from_le_bytes(buff))
    }

    pub fn read_u64(&mut self) -> Result<u64, SimpleDbError> {
        let mut buff = [0u8; 8];
        self.tcp_stream.read_exact(&mut buff)
            .map_err(|e| CannotDecodeNetworkMessage(String::from("Cannot read message u64")))?;
        Ok(u64::from_le_bytes(buff))
    }

    pub fn read_n(&mut self, n: usize) -> Result<Vec<u8>, SimpleDbError>{
        let mut buff = vec![0u8; n];
        self.tcp_stream.read_exact(&mut buff)
            .map_err(|e| CannotDecodeNetworkMessage(String::from("Cannot read message u64")))?;
        Ok(buff)
    }

    pub fn write(&mut self, bytes: Vec<u8>) -> Result<usize, SimpleDbError> {
        self.tcp_stream.write(bytes.as_slice())
            .map_err(|_| CannotDecodeNetworkMessage(String::from("Cannot write to socket")))
    }
}