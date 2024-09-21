use std::cmp::max;
use bytes::Bytes;
use shared::{utils, SimpleDbError};

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
    pub fn get_arithmetic_produced_type(&self, other: ColumnType) -> ColumnType {
        let bytes_self = self.get_numeric_bytes();
        let bytes_other = other.get_numeric_bytes();
        let result_type_bytes = max(bytes_self, bytes_other);

        if self.is_fp_number() || other.is_fp_number() {
            Self::get_fp_number_column_type_by_bytes(result_type_bytes)
        } else if self.is_signed_number() || other.is_signed_number() {
            Self::get_signed_number_column_type_by_bytes(result_type_bytes)
        } else {
            Self::get_unsigned_number_column_type_by_bytes(result_type_bytes)
        }
    }

    fn is_signed_number(&self) -> bool {
        match self {
            ColumnType::I8 => true,
            ColumnType::I16 => true,
            ColumnType::I32 => true,
            ColumnType::I64 => true,
            ColumnType::U8 => false,
            ColumnType::U16 => false,
            ColumnType::U32 => false,
            ColumnType::U64 => false,
            ColumnType::F32 => true,
            ColumnType::F64 => true,
            _ => false
        }
    }

    fn is_fp_number(&self) -> bool {
        match self {
            ColumnType::F32 |
            ColumnType::F64 => true,
            _ => false
        }
    }

    fn get_numeric_bytes(&self) -> usize {
        match self {
            ColumnType::I8 => 1,
            ColumnType::U8 => 1,
            ColumnType::I16 => 2,
            ColumnType::U16 => 2,
            ColumnType::U32 => 4,
            ColumnType::I32 => 4,
            ColumnType::U64 => 8,
            ColumnType::I64 => 8,
            ColumnType::F32 => 4,
            ColumnType::F64 => 8,
            _ => 0,
        }
    }

    pub fn is_comparable(&self, other: ColumnType) -> bool {
        if self.is_numeric() && other.is_numeric() {
            true
        } else {
            *self == other
        }
    }

    pub fn is_numeric(&self) -> bool {
        match self {
            ColumnType::I8 |
            ColumnType::U8 |
            ColumnType::I16 |
            ColumnType::U16 |
            ColumnType::U32 |
            ColumnType::I32 |
            ColumnType::U64 |
            ColumnType::I64 |
            ColumnType::F32 |
            ColumnType::F64 => true,
            _ => false
        }
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, ColumnType::BOOLEAN)
    }

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

    fn get_fp_number_column_type_by_bytes(bytes: usize) -> ColumnType {
        if bytes == 4 {
            ColumnType::F32
        } else {
            ColumnType::F64
        }
    }

    fn get_signed_number_column_type_by_bytes(bytes: usize) -> ColumnType {
        match bytes {
            1 => ColumnType::I8,
            2 => ColumnType::I16,
            4 => ColumnType::I32,
            8 => ColumnType::I64,
            _ => panic!(""),
        }
    }

    fn get_unsigned_number_column_type_by_bytes(bytes: usize) -> ColumnType {
        match bytes {
            1 => ColumnType::U8,
            2 => ColumnType::U16,
            4 => ColumnType::U32,
            8 => ColumnType::U64,
            _ => panic!(""),
        }
    }
}