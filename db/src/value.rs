use bytes::Bytes;
use shared::SimpleDbError::MalformedQuery;
use shared::{utils, SimpleDbError};

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Type {
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
    Boolean,
    String,
    Date,
    Blob,
    Null
}

impl Type {
    pub fn serialize(&self) -> u8 {
        match &self {
            Type::I8 => 1,
            Type::U8 => 2,
            Type::I16 => 3,
            Type::U16 => 4,
            Type::U32 => 5,
            Type::I32 => 6,
            Type::U64 => 7,
            Type::I64 => 8,
            Type::F32 => 9,
            Type::F64 => 10,
            Type::Boolean => 11,
            Type::String => 12,
            Type::Date => 13,
            Type::Blob => 14,
            Type::Null => panic!("Illegal code path")
        }
    }

    pub fn deserialize(value: u8) -> Result<Type, u8> {
        match value {
            1 =>  Ok(Type::I8),
            2 =>  Ok(Type::U8),
            3 =>  Ok(Type::I16),
            4 =>  Ok(Type::U16),
            5 =>  Ok(Type::U32),
            6 =>  Ok(Type::I32),
            7 =>  Ok(Type::U64),
            8 =>  Ok(Type::I64),
            9 =>  Ok(Type::F32),
            10 => Ok(Type::F64) ,
            11 => Ok(Type::Boolean),
            12 => Ok(Type::String),
            13 => Ok(Type::Date),
            14 => Ok(Type::Blob),
            _ => Err(value)
        }
    }

    pub fn can_be_casted(&self, other: &Type) -> bool {
        if self.is_fp_number() && other.is_fp_number() {
            true
        } else if self.is_integer_number() && other.is_integer_number() {
            true
        } else if self.is_null() || other.is_null() {
            true
        } else {
            utils::enum_eq(self, other)
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Type::Null)
    }

    pub fn is_fp_number(&self) -> bool {
        matches!(self, Type::F64) || matches!(self, Type::F32)
    }

    pub fn is_signed_integer_number(&self) -> bool {
        match self {
            Type::I8 | Type::I16 | Type::I32 | Type::I64 => true,
            _ => false
        }
    }

    pub fn is_integer_number(&self) -> bool {
        self.is_signed_integer_number() || self.is_unsigned_integer_number()
    }

    pub fn is_unsigned_integer_number(&self) -> bool {
        match self {
            Type::U8 | Type::U16 | Type::U32 | Type::U64 => true,
            _ => false
        }
    }

    pub fn is_number(&self) -> bool {
        self.is_signed_integer_number() || self.is_unsigned_integer_number() || self.is_fp_number()
    }

    pub fn is_comparable(&self, other: &Type) -> bool {
        //Null types can always be compared
        if (self.is_number() && other.is_number()) || matches!(other, Type::Null) {
            true
        } else {
            utils::enum_eq(self, &other)
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    I8(i8),
    U8(u8),
    I16(i16),
    U16(u16),
    U32(u32),
    I32(i32),
    U64(u64),
    I64(i64),
    F32(f32),
    F64(f64),
    Boolean(bool),
    String(String),
    Date,
    Blob(Bytes),
    Null
}

impl Value {
    pub fn deserialize(bytes: Bytes, expected_type: Type) -> Result<Value, ()> {
        if expected_type.is_number() && (bytes.len() > 8 || bytes.len() == 0)  {
            return Err(());
        }

        match expected_type {
            Type::I8 => Ok(Value::I8(utils::bytes_to_i8(&bytes))),
            Type::U8 => Ok(Value::U8(utils::bytes_to_u8(&bytes))),
            Type::I16 => Ok(Value::I16(utils::bytes_to_i16_le(&bytes))),
            Type::U16 => Ok(Value::U16(utils::bytes_to_u16_le(&bytes))),
            Type::U32 => Ok(Value::U32(utils::bytes_to_u32_le(&bytes))),
            Type::I32 => Ok(Value::I32(utils::bytes_to_i32_le(&bytes))),
            Type::U64 => Ok(Value::U64(utils::bytes_to_u64_le(&bytes))),
            Type::I64 => Ok(Value::I64(utils::bytes_to_i64_le(&bytes))),
            Type::F32 => Ok(Value::F32(utils::bytes_to_f32_le(&bytes))),
            Type::F64 => Ok(Value::F64(utils::bytes_to_f64_le(&bytes))),
            Type::Boolean => Ok(Value::Boolean(bytes[0] != 0x00)),
            Type::String => String::from_utf8(bytes.to_vec())
                .map_err(|_| ())
                .map(|string| Value::String(string)),
            Type::Date => todo!(),
            Type::Blob => Ok(Value::Blob(bytes)),
            Type::Null => Ok(Value::Null)
        }
    }

    pub fn serialize(&self) -> Bytes {
        match &self {
            Value::I8(value) => Bytes::copy_from_slice(value.to_le_bytes().as_slice()),
            Value::U8(value) => Bytes::copy_from_slice(value.to_le_bytes().as_slice()),
            Value::I16(value) => Bytes::copy_from_slice(value.to_le_bytes().as_slice()),
            Value::U16(value) => Bytes::copy_from_slice(value.to_le_bytes().as_slice()),
            Value::U32(value) => Bytes::copy_from_slice(value.to_le_bytes().as_slice()),
            Value::I32(value) => Bytes::copy_from_slice(value.to_le_bytes().as_slice()),
            Value::U64(value) => Bytes::copy_from_slice(value.to_le_bytes().as_slice()),
            Value::I64(value) => Bytes::copy_from_slice(value.to_le_bytes().as_slice()),
            Value::F32(value) => Bytes::copy_from_slice(value.to_le_bytes().as_slice()),
            Value::F64(value) => Bytes::from((*value).to_le_bytes().to_vec()),
            Value::Boolean(value) => {
                if *value {
                    Bytes::from(vec![0x01])
                } else {
                    Bytes::from(vec![0x00])
                }
            }
            Value::String(value) => Bytes::copy_from_slice(value.as_bytes()),
            Value::Date => todo!(),
            Value::Blob(value) => value.clone(),
            Value::Null => Bytes::from(vec![])
        }
    }

    pub fn to_type(&self) -> Type {
        match &self {
            Value::I8(_) => Type::I8,
            Value::U8(_) => Type::U8,
            Value::I16(_) => Type::I16,
            Value::U16(_) => Type::U16,
            Value::U32(_) => Type::U32,
            Value::I32(_) => Type::I32,
            Value::U64(_) => Type::U64,
            Value::I64(_) => Type::I64,
            Value::F32(_) => Type::F32,
            Value::F64(_) => Type::F64,
            Value::Boolean(_) => Type::Boolean,
            Value::String(_) => Type::String,
            Value::Date => Type::Date,
            Value::Blob(_) => Type::Blob,
            Value::Null => Type::Null,
        }
    }

    pub fn get_f64(&self) -> Result<f64, SimpleDbError> {
        match self {
            Value::I8(value) => Ok(*value as f64),
            Value::U8(value) => Ok(*value as f64),
            Value::I16(value) => Ok(*value as f64),
            Value::U16(value) => Ok(*value as f64),
            Value::U32(value) => Ok(*value as f64),
            Value::I32(value) => Ok(*value as f64),
            Value::U64(value) => Ok(*value as f64),
            Value::I64(value) => Ok(*value as f64),
            Value::F32(value) => Ok(*value as f64),
            Value::F64(value) => Ok(*value),
            Value::Boolean(value) => if *value { Ok(1.0) } else { Ok(0.0) },
            Value::String(_) |
            Value::Date |
            Value::Blob(_) |
            //TODO Improve error
            Value::Null => Err(SimpleDbError::MalformedQuery(String::from("Cannot get as f64")))
        }
    }

    pub fn get_i64(&self) -> Result<i64, SimpleDbError> {
        match self {
            Value::I8(value) => Ok(*value as i64),
            Value::U8(value) => Ok(*value as i64),
            Value::I16(value) => Ok(*value as i64),
            Value::U16(value) => Ok(*value as i64),
            Value::U32(value) => Ok(*value as i64),
            Value::I32(value) => Ok(*value as i64),
            Value::U64(value) => Ok(*value as i64),
            Value::I64(value) => Ok(*value),
            Value::F32(value) => Ok(*value as i64),
            Value::F64(value) => Ok(*value as i64),
            Value::Boolean(value) => if *value { Ok(1) } else { Ok(0) },
            Value::String(_) |
            Value::Date |
            Value::Blob(_) |
            //TODO Improve error
            Value::Null => Err(SimpleDbError::MalformedQuery(String::from("Cannot get as f64")))
        }
    }

    pub fn get_string(&self) -> Result<&String, SimpleDbError> {
        match &self {
            Value::String(value) => Ok(value),
            _ => Err(MalformedQuery(String::from("Cannot get String")))
        }
    }

    pub fn get_boolean(&self) -> Result<bool, SimpleDbError> {
        match &self {
            Value::Boolean(value) => Ok(*value),
            _ => Err(MalformedQuery(String::from("Cannot get String")))
        }
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, Value::Boolean(_))
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    pub fn to_string(&self) -> String {
        match &self {
            Value::I8(value) => value.to_string(),
            Value::U8(value) => value.to_string(),
            Value::I16(value) => value.to_string(),
            Value::U16(value) => value.to_string(),
            Value::U32(value) => value.to_string(),
            Value::I32(value) => value.to_string(),
            Value::U64(value) => value.to_string(),
            Value::I64(value) => value.to_string(),
            Value::F32(value) => format!("{:.2}", value.to_string()),
            Value::F64(value) => format!("{:.2}", value.to_string()),
            Value::Boolean(value) => if *value { String::from("false") } else { String::from("true") }
            Value::String(value) => value.clone(),
            Value::Date => todo!(),
            Value::Blob(value) => format!("{:02X?}", value.to_vec()),
            Value::Null => "Null".to_string()
        }
    }

    pub fn is_number(&self) -> bool {
        match &self {
            Value::I8(_) |
            Value::U8(_) |
            Value::I16(_) |
            Value::U16(_) |
            Value::U32(_) |
            Value::I32(_) |
            Value::U64(_) |
            Value::I64(_) |
            Value::F32(_) |
            Value::F64(_) => true,
            Value::Boolean(_) |
            Value::String(_) |
            Value::Date |
            Value::Blob(_) |
            Value::Null => false
        }
    }

    pub fn is_integer_number(&self) -> bool {
        match &self {
            Value::I8(_) |
            Value::U8(_) |
            Value::I16(_) |
            Value::U16(_) |
            Value::U32(_) |
            Value::I32(_) |
            Value::U64(_) |
            Value::I64(_) => true,
            _ => false
        }
    }

    pub fn is_fp_number(&self) -> bool {
        match &self {
            Value::F32(_) |
            Value::F64(_) => true,
            _ => false
        }
    }

    pub fn is_comparable(&self, other: &Value) -> bool {
        if self.is_number() && other.is_number() {
            true
        } else {
            utils::enum_eq(self, other)
        }
    }

    pub fn and(&self, other: &Value) -> Result<Value, SimpleDbError> {
        if self.is_boolean() && other.is_boolean() {
            Ok(Value::Boolean(self.get_boolean()? && other.get_boolean()?))
        } else {
            Err(SimpleDbError::MalformedQuery(String::from("Cannot and values")))
        }
    }

    pub fn or(&self, other: &Value) -> Result<Value, SimpleDbError> {
        if self.is_boolean() && other.is_boolean() {
            Ok(Value::Boolean(self.get_boolean()? || other.get_boolean()?))
        } else {
            Err(SimpleDbError::MalformedQuery(String::from("Cannot or values")))
        }
    }

    pub fn greater(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.comparation_op(other, |a, b| a > b, |a, b| a > b, |a, b| a > b)
    }

    pub fn greater_equal(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.comparation_op(other, |a, b| a >= b, |a, b| a >= b, |a, b| a >= b)
    }

    pub fn less(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.comparation_op(other, |a, b| a < b, |a, b| a < b, |a, b| a < b)
    }

    pub fn less_equal(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.comparation_op(other, |a, b| a <= b, |a, b| a <= b, |a, b| a <= b)
    }

    pub fn equal(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.comparation_op(other, |a, b| a == b, |a, b| a == b, |a, b| a == b)
    }

    pub fn not_equal(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.comparation_op(other, |a, b| a != b, |a, b| a != b, |a, b| a != b)
    }

    pub fn add(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a + b, |a, b| a + b)
    }

    pub fn substract(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a - b, |a, b| a - b)
    }

    pub fn multiply(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a * b, |a, b| a * b)
    }

    pub fn divide(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a / b, |a, b| a / b)
    }

    fn arithmetic_op<FpOp, IntOp>(&self, other: &Value, fp_op: FpOp, int_op: IntOp) -> Result<Value, SimpleDbError>
    where
        IntOp: Fn(i64, i64) -> i64,
        FpOp: Fn(f64, f64) -> f64,
    {
        if !self.is_number() && !other.is_number() {
            return Err(MalformedQuery(String::from("Only numbers can be added")));
        }

        if !self.is_fp_number() && !other.is_fp_number() {
            Ok(Value::I64(int_op(self.get_i64()?, other.get_i64()?)))
        } else {
            Ok(Value::F64(fp_op(self.get_f64()?, other.get_f64()?)))
        }
    }

    fn comparation_op<FpOp, IntOp, StrOp>(
        &self,
        other: &Value,
        fp_op: FpOp,
        int_op: IntOp,
        str_op: StrOp
    ) -> Result<Value, SimpleDbError>
    where
        StrOp: Fn(&String, &String) -> bool,
        IntOp: Fn(i64, i64) -> bool,
        FpOp: Fn(f64, f64) -> bool,
    {
        if !self.is_comparable(other) {
            return Err(SimpleDbError::MalformedQuery(String::from("Cannot compare values")));
        }

        if self.is_fp_number() && other.is_fp_number() {
            Ok(Value::Boolean(fp_op(self.get_f64()?, other.get_f64()?)))
        } else if self.is_integer_number() && other.is_integer_number() {
            Ok(Value::Boolean(int_op(self.get_i64()?, other.get_i64()?)))
        } else if self.is_string() && other.is_string() {
            Ok(Value::Boolean(str_op(self.get_string()?, other.get_string()?)))
        } else {
            Err(SimpleDbError::MalformedQuery(String::from("Cannot compare values")))
        }
    }
}