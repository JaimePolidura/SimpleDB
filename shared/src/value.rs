use std::cmp::Ordering;
use crate::SimpleDbError::{IllegalTypeCastFromBytes, IllegalTypeOperation};
use crate::{utils, SimpleDbError, TypeId};
use bytes::Bytes;

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd)]
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
    pub fn serialize(&self) -> TypeId {
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

    pub fn to_string(&self) -> String {
        match &self {
            Type::I8 => "I8".to_string(),
            Type::U8 => "U8".to_string(),
            Type::I16 => "I16".to_string(),
            Type::U16 => "U16".to_string(),
            Type::U32 => "U32".to_string(),
            Type::I32 => "I32".to_string(),
            Type::U64 => "U64".to_string(),
            Type::I64 => "I64".to_string(),
            Type::F32 => "F32".to_string(),
            Type::F64 => "F64".to_string(),
            Type::Boolean => "Boolean".to_string(),
            Type::String => "String".to_string(),
            Type::Date => "Date".to_string(),
            Type::Blob => "Blob".to_string(),
            Type::Null => "Null".to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Value {
    value_type: Type,
    value_bytes: Bytes,
}

impl Value {
    pub fn create(value_bytes: Bytes, value_type: Type) -> Result<Value, SimpleDbError> {
        if !Self::bytes_has_type_format(&value_bytes, &value_type) {
            return Err(IllegalTypeCastFromBytes(value_type));
        }

        Ok(Value {
            value_bytes,
            value_type
        })
    }

    pub fn create_boolean(boolean_value: bool) -> Value {
        let value_bytes = if boolean_value {
            Bytes::from(vec![0x01])
        } else {
            Bytes::from(vec![0x00])
        };

        Value {
            value_type: Type::Boolean,
            value_bytes,
        }
    }

    pub fn create_f64(value_f64: f64) -> Value {
        Value {
            value_bytes: Bytes::from(value_f64.to_le_bytes().to_vec()),
            value_type: Type::F64,
        }
    }

    pub fn create_i64(value_i64: i64) -> Value {
        Value {
            value_bytes: Bytes::from(value_i64.to_le_bytes().to_vec()),
            value_type: Type::I64,
        }
    }

    pub fn create_null() -> Value {
        Value {
            value_bytes: Bytes::from(vec![]),
            value_type: Type::Null
        }
    }

    pub fn create_string(string: String) -> Value {
        Value {
            value_bytes: Bytes::from(string.as_bytes().to_vec()),
            value_type: Type::String
        }
    }

    pub fn get_bytes(&self) -> &Bytes {
        &self.value_bytes
    }

    pub fn get_type(&self) -> Type {
        self.value_type.clone()
    }

    pub fn get_f64(&self) -> Result<f64, SimpleDbError> {
        match self.value_type {
            Type::F64 => Ok(utils::bytes_to_f64_le(&self.value_bytes)),
            Type::F32 => Ok(utils::bytes_to_f64_le(&self.value_bytes)),
            Type::I64 => Ok(utils::bytes_to_i64_le(&self.value_bytes) as f64),
            Type::U64 => Ok(utils::bytes_to_i64_le(&self.value_bytes) as f64),
            Type::I32 => Ok(utils::bytes_to_i32_le(&self.value_bytes) as f64),
            Type::U32 => Ok(utils::bytes_to_i32_le(&self.value_bytes) as f64),
            Type::I16 => Ok(utils::bytes_to_i16_le(&self.value_bytes) as f64),
            Type::U16 => Ok(utils::bytes_to_i16_le(&self.value_bytes) as f64),
            Type::I8 => Ok(self.value_bytes[0] as f64),
            Type::U8 => Ok(self.value_bytes[0] as f64),
            Type::Boolean => if self.value_bytes[0] != 0x00 { Ok(1.0) } else { Ok(0.0) },
            Type::String |
            Type::Date |
            Type::Blob |
            Type::Null => Err(SimpleDbError::IllegalTypeOperation("Expected number type from value")),
        }
    }

    pub fn get_i64(&self) -> Result<i64, SimpleDbError> {
        match self.value_type {
            Type::F64 => Ok(utils::bytes_to_f64_le(&self.value_bytes) as i64),
            Type::F32 => Ok(utils::bytes_to_f64_le(&self.value_bytes) as i64),
            Type::I64 => Ok(utils::bytes_to_i64_le(&self.value_bytes)),
            Type::U64 => Ok(utils::bytes_to_i64_le(&self.value_bytes)),
            Type::I32 => Ok(utils::bytes_to_i32_le(&self.value_bytes) as i64),
            Type::U32 => Ok(utils::bytes_to_i32_le(&self.value_bytes) as i64),
            Type::I16 => Ok(utils::bytes_to_i16_le(&self.value_bytes) as i64),
            Type::U16 => Ok(utils::bytes_to_i16_le(&self.value_bytes) as i64),
            Type::I8 => Ok(self.value_bytes[0] as i64),
            Type::U8 => Ok(self.value_bytes[0] as i64),
            Type::Boolean => if self.value_bytes[0] != 0x00 { Ok(1) } else { Ok(0) },
            Type::String |
            Type::Date |
            Type::Blob |
            Type::Null => Err(SimpleDbError::IllegalTypeOperation("Expected number type from value")),
        }
    }

    pub fn get_string(&self) -> Result<String, SimpleDbError> {
        match self.value_type {
            Type::String => Ok(String::from_utf8(self.value_bytes.to_vec()).unwrap()),
            _ => Err(IllegalTypeOperation("Expected string type"))
        }
    }

    pub fn get_boolean(&self) -> Result<bool, SimpleDbError> {
        Ok(self.get_i64()? != 0x00)
    }

    pub fn is_string(&self) -> bool {
        matches!(self.value_type, Type::String)
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self.value_type, Type::Boolean)
    }

    pub fn is_null(&self) -> bool {
        matches!(self.value_type, Type::Null)
    }

    pub fn to_string(&self) -> String {
        match &self.value_type {
            Type::I8 => utils::bytes_to_i8(&self.value_bytes).to_string(),
            Type::U8 => utils::bytes_to_u8(&self.value_bytes).to_string(),
            Type::I16 => utils::bytes_to_i16_le(&self.value_bytes).to_string(),
            Type::U16 => utils::bytes_to_u16_le(&self.value_bytes).to_string(),
            Type::U32 => utils::bytes_to_u32_le(&self.value_bytes).to_string(),
            Type::I32 => utils::bytes_to_i32_le(&self.value_bytes).to_string(),
            Type::U64 => utils::bytes_to_u64_le(&self.value_bytes).to_string(),
            Type::I64 => utils::bytes_to_i64_le(&self.value_bytes).to_string(),
            Type::F32 => utils::bytes_to_f32_le(&self.value_bytes).to_string(),
            Type::F64 => utils::bytes_to_f64_le(&self.value_bytes).to_string(),
            Type::Boolean => if self.value_bytes[0] == 0x01 { String::from("true") } else { String::from("false") },
            Type::String => String::from_utf8(self.value_bytes.to_vec()).unwrap(),
            Type::Date => todo!(),
            Type::Blob => format!("{:02X?}", self.value_bytes),
            Type::Null => "Null".to_string()
        }
    }

    pub fn is_number(&self) -> bool {
        match &self.value_type {
            Type::I8 |
            Type::U8 |
            Type::I16 |
            Type::U16 |
            Type::U32 |
            Type::I32 |
            Type::U64 |
            Type::I64 |
            Type::F32 |
            Type::F64 => true,
            Type::Boolean |
            Type::String |
            Type::Date |
            Type::Blob |
            Type::Null => false
        }
    }

    pub fn is_integer_number(&self) -> bool {
        match &self.value_type {
            Type::I8 |
            Type::U8 |
            Type::I16 |
            Type::U16 |
            Type::U32 |
            Type::I32 |
            Type::U64 |
            Type::I64 => true,
            _ => false
        }
    }

    pub fn is_fp_number(&self) -> bool {
        match &self.value_type {
            Type::F32 |
            Type::F64 => true,
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
            let boolean_result = self.get_boolean()? && other.get_boolean()?;
            Ok(Self::create_boolean(boolean_result))
        } else {
            Err(SimpleDbError::IllegalTypeOperation("Cannot and values"))
        }
    }

    pub fn or(&self, other: &Value) -> Result<Value, SimpleDbError> {
        if self.is_boolean() && other.is_boolean() {
            Ok(Self::create_boolean(self.get_boolean()? || other.get_boolean()?))
        } else {
            Err(SimpleDbError::IllegalTypeOperation("Cannot or values"))
        }
    }

    pub fn gt_bytes(&self, other: &Bytes) -> bool {
        let other_value = Value::create(other.clone(), self.value_type.clone()).unwrap();
        self.comparation_op(&other_value, |a, b| a > b, |a, b| a > b, |a, b| a > b)
            .unwrap()
    }


    pub fn ge_bytes(&self, other: &Bytes) -> bool {
        let other_value = Value::create(other.clone(), self.value_type.clone()).unwrap();
        self.comparation_op(&other_value, |a, b| a >= b, |a, b| a >= b, |a, b| a >= b)
            .unwrap()
    }

    pub fn lt_bytes(&self, other: &Bytes) -> bool {
        let other_value = Value::create(other.clone(), self.value_type.clone()).unwrap();
        self.comparation_op(&other_value, |a, b| a < b, |a, b| a < b, |a, b| a < b)
            .unwrap()
    }

    pub fn le_bytes(&self, other: &Bytes) -> bool {
        let other_value = Value::create(other.clone(), self.value_type.clone()).unwrap();
        self.comparation_op(&other_value, |a, b| a <= b, |a, b| a <= b, |a, b| a <= b)
            .unwrap()
    }

    pub fn add(&self, other: &Value) -> Result<Value, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a + b, |a, b| a + b)
    }

    pub fn subtract(&self, other: &Value) -> Result<Value, SimpleDbError> {
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
            return Err(IllegalTypeOperation("Only numbers can be added"));
        }

        if !self.is_fp_number() && !other.is_fp_number() {
            let result = int_op(self.get_i64()?, other.get_i64()?);
            Value::create(Bytes::from(result.to_le_bytes().to_vec()), Type::I64)
        } else {
            let result = fp_op(self.get_f64()?, other.get_f64()?);
            Value::create(Bytes::from(result.to_le_bytes().to_vec()), Type::F64)
        }
    }

    fn comparation_op<FpOp, IntOp, StrOp>(
        &self,
        other: &Value,
        fp_op: FpOp,
        int_op: IntOp,
        str_op: StrOp
    ) -> Result<bool, SimpleDbError>
    where
        StrOp: Fn(&String, &String) -> bool,
        IntOp: Fn(i64, i64) -> bool,
        FpOp: Fn(f64, f64) -> bool,
    {
        if !self.is_comparable(other) {
            return Err(SimpleDbError::IllegalTypeOperation("Cannot compare values"));
        }

        if self.is_fp_number() && other.is_fp_number() {
            Ok(fp_op(self.get_f64()?, other.get_f64()?))
        } else if self.is_integer_number() && other.is_integer_number() {
            Ok(int_op(self.get_i64()?, other.get_i64()? ))
        } else if self.is_string() && other.is_string() {
            Ok(str_op(&self.get_string()?, &other.get_string()?))
        } else {
            Err(SimpleDbError::IllegalTypeOperation("Cannot compare values"))
        }
    }

    fn bytes_has_type_format(bytes: &Bytes, expected_type: &Type) -> bool {
        match expected_type {
            Type::I8 => bytes.len() <= 8,
            Type::U8 => bytes.len() <= 8,
            Type::I16 => bytes.len() <= 8,
            Type::U16 => bytes.len() <= 8,
            Type::U32 => bytes.len() <= 8,
            Type::I32 => bytes.len() <= 8,
            Type::U64 => bytes.len() <= 8,
            Type::I64 => bytes.len() <= 8,
            Type::F32 => bytes.len() <= 8,
            Type::F64 => bytes.len() <= 8,
            Type::Boolean => true,
            Type::String => String::from_utf8(bytes.to_vec()).is_ok(),
            Type::Date => todo!(),
            Type::Blob => true,
            Type::Null => true,
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.get_bytes().eq(other.get_bytes()) {
            return Ordering::Equal;
        }

        let is_less = self.comparation_op(
            &other,
            |a, b| a < b,
            |a, b| a < b,
            |a, b| a < b
        ).unwrap();

        if is_less {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if !self.get_type().is_comparable(&other.get_type()) {
            return None;
        }
        if self.get_bytes().eq(other.get_bytes()) {
            return Some(Ordering::Equal);
        }

        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod test {
    use crate::Value;

    #[test]
    fn compare_strings() {
        let a = Value::create_string("a".to_string());
        let b = Value::create_string("b".to_string());
        assert!(!a.gt(&b));
        assert!(!a.ge(&b));
        assert!(!a.eq(&b));
        assert!(a.le(&b));
        assert!(a.lt(&b));

        let a = Value::create_string("a".to_string());
        let b = Value::create_string("a".to_string());
        assert!(!a.gt(&b));
        assert!(a.ge(&b));
        assert!(a.eq(&b));
        assert!(a.le(&b));
        assert!(!a.lt(&b));
    }

    #[test]
    fn compare_numbers() {
        let a = Value::create_i64(143);
        let b = Value::create_i64(13);
        assert!(a.gt(&b));
        assert!(a.ge(&b));
        assert!(!a.eq(&b));
        assert!(!a.le(&b));
        assert!(!a.lt(&b));
    }
}