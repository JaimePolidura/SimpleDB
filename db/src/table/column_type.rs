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
    VARCHAR,
    DATE,
    BLOB
}

impl ColumnType {
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