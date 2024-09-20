use bytes::Bytes;
use shared::utils;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ColumnType {
    I8,
    U8,
    I16,
    U16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    BOOLEAN,
    VARCHAR, //AKA Strings
    DATE, //TODO
    BLOB //TODO
}

impl ColumnType {
    pub fn has_valid_format(&self, bytes: &Bytes) -> bool {
        match *self {
            ColumnType::I8 => bytes.len() <= 8 && !utils::overflows_bytes_64(&bytes, 1),
            ColumnType::U8 => bytes.len() <= 8 && !utils::overflows_bytes_64(&bytes, 1),
            ColumnType::I16 => bytes.len() <= 8 && !utils::overflows_bytes_64(&bytes, 2),
            ColumnType::U16 => bytes.len() <= 8 && !utils::overflows_bytes_64(&bytes, 2),
            ColumnType::U32 => bytes.len() <= 8 && !utils::overflows_bytes_64(&bytes, 4),
            ColumnType::I32 => bytes.len() <= 8 && !utils::overflows_bytes_64(&bytes, 4),
            ColumnType::U64 => bytes.len() <= 8 && !utils::overflows_bytes_64(&bytes, 8),
            ColumnType::I64 => bytes.len() <= 8 && !utils::overflows_bytes_64(&bytes, 8),
            ColumnType::F32 => bytes.len() <= 8 && !utils::overflows_bytes_64(&bytes, 4),
            ColumnType::F64 => bytes.len() <= 8 && !utils::overflows_bytes_64(&bytes, 8),
            ColumnType::BOOLEAN => {
                let vec = bytes.to_vec();
                vec.len() == 1 && (vec[0] == 0x00 || vec[1] == 0x01)
            },
            ColumnType::VARCHAR => String::from_utf8(bytes.to_vec()).is_ok(),
            ColumnType::DATE => todo!(),
            ColumnType::BLOB => true,
        }
    }

    pub fn serialize(&self) -> u8 {
        match *self {
            ColumnType::I8 => 1,
            ColumnType::U8 => 2,
            ColumnType::I16 => 3,
            ColumnType::U16 => 4,
            ColumnType::U32 => 5,
            ColumnType::I32 => 6,
            ColumnType::U64 => 7,
            ColumnType::I64 => 8,
            ColumnType::F32 => 9,
            ColumnType::F64 => 10,
            ColumnType::BOOLEAN => 11,
            ColumnType::VARCHAR => 12,
            ColumnType::DATE => 13,
            ColumnType::BLOB => 14,
        }
    }

    pub fn deserialize(value: u8) -> Result<ColumnType, u8> {
        match value {
            1 =>  Ok(ColumnType::I8),
            2 =>  Ok(ColumnType::U8),
            3 =>  Ok(ColumnType::I16),
            4 =>  Ok(ColumnType::U16),
            5 =>  Ok(ColumnType::U32),
            6 =>  Ok(ColumnType::I32),
            7 =>  Ok(ColumnType::U64),
            8 =>  Ok(ColumnType::I64),
            9 =>  Ok(ColumnType::F32),
            10 => Ok(ColumnType::F64) ,
            11 => Ok(ColumnType::BOOLEAN),
            12 => Ok(ColumnType::VARCHAR),
            13 => Ok(ColumnType::DATE),
            14 => Ok(ColumnType::BLOB),
            _ => Err((value))
        }
    }
}