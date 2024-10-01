use crate::SimpleDbError::NetworkError;
use crate::{ConnectionId, SimpleDbError};
use std::hash::{DefaultHasher, Hasher};
use std::io::{Read, Write};
use std::net::{IpAddr, TcpStream};

pub struct Connection {
    tcp_stream: TcpStream
}

impl Connection {
    pub fn create(tcp_stream: TcpStream) -> Connection {
        Connection { tcp_stream }
    }

    pub fn connection_id(&self) -> ConnectionId {
        let address = self.tcp_stream.peer_addr().unwrap();
        let port = address.port();

        match address.ip() {
            IpAddr::V4(ipv4) => (u32::from(ipv4) as ConnectionId) << 32 | address.port() as ConnectionId,
            IpAddr::V6(ipv6) => {
                let mut hasher = DefaultHasher::new();
                hasher.write_u16(port);
                hasher.write_u128(u128::from_be_bytes(ipv6.octets()));
                hasher.finish() as ConnectionId
            },
        }
    }

    pub fn address(&self) -> String {
        self.tcp_stream.peer_addr().unwrap().to_string()
    }

    pub fn read_u8(&mut self) -> Result<u8, SimpleDbError> {
        let mut buf = [0u8; 1];
        self.tcp_stream.read_exact(&mut buf)
            .map_err(|e| NetworkError(e))?;
        Ok(buf[0])
    }

    pub fn read_u16(&mut self) -> Result<u16, SimpleDbError> {
        let mut buff = [0u8; 2];
        self.tcp_stream.read_exact(&mut buff)
            .map_err(|e| NetworkError(e))?;
        Ok(u16::from_le_bytes(buff))
    }

    pub fn read_u32(&mut self) -> Result<u32, SimpleDbError> {
        let mut buff = [0u8; 4];
        self.tcp_stream.read_exact(&mut buff)
            .map_err(|e| NetworkError(e))?;
        Ok(u32::from_le_bytes(buff))
    }

    pub fn read_u64(&mut self) -> Result<u64, SimpleDbError> {
        let mut buff = [0u8; 8];
        self.tcp_stream.read_exact(&mut buff)
            .map_err(|e| NetworkError(e))?;
        Ok(u64::from_le_bytes(buff))
    }

    pub fn read_n(&mut self, n: usize) -> Result<Vec<u8>, SimpleDbError> {
        let mut buff = vec![0u8; n];
        self.tcp_stream.read_exact(&mut buff)
            .map_err(|e| NetworkError(e))?;
        Ok(buff)
    }

    pub fn write(&mut self, bytes: Vec<u8>) -> Result<usize, SimpleDbError> {
        self.tcp_stream.write(bytes.as_slice())
            .map_err(|e| NetworkError(e))
    }
}