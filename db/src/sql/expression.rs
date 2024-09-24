use crate::ColumnType;
use bytes::Bytes;
use shared::{utils, SimpleDbError};
use std::cmp::PartialEq;
use std::f32::consts::E;
use SimpleDbError::MalformedQuery;

#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    None,

    Binary(BinaryOperator, Box<Expression>, Box<Expression>),
    Unary(UnaryOperator, Box<Expression>),
    Identifier(String),
    String(String),
    Boolean(bool),
    NumberF64(f64),
    NumberI64(i64),
    Null,
}

#[derive(Clone, Debug, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    And,
    Or,
    NotEqual,
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

impl Expression {
    pub fn get_boolean(&self) -> Result<bool, SimpleDbError> {
        match self {
            Expression::Boolean(value) => Ok(*value),
            _ => Err(MalformedQuery(String::from("Cannot get boolean value from expression")))
        }
    }

    pub fn deserialize(
        column_type: ColumnType,
        bytes: &Bytes,
    ) -> Result<Expression, SimpleDbError> {
        match column_type {
            ColumnType::I8 => Ok(Expression::NumberI64(utils::bytes_to_i8(bytes) as i64)),
            ColumnType::U8 => Ok(Expression::NumberI64(utils::bytes_to_u8(bytes) as i64)),
            ColumnType::I16 => Ok(Expression::NumberI64(utils::bytes_to_i16_le(bytes) as i64)),
            ColumnType::U16 => Ok(Expression::NumberI64(utils::bytes_to_u16_le(bytes) as i64)),
            ColumnType::U32 => Ok(Expression::NumberI64(utils::bytes_to_u32_le(bytes) as i64)),
            ColumnType::I32 => Ok(Expression::NumberI64(utils::bytes_to_i32_le(bytes) as i64)),
            ColumnType::U64 => Ok(Expression::NumberI64(utils::bytes_to_u64_le(bytes) as i64)),
            ColumnType::I64 => Ok(Expression::NumberI64(utils::bytes_to_i64_le(bytes))),
            ColumnType::F32 => Ok(Expression::NumberF64(utils::bytes_to_f32_le(bytes) as f64)),
            ColumnType::F64 => Ok(Expression::NumberF64(utils::bytes_to_f64_le(bytes))),
            ColumnType::Boolean => Ok(Expression::Boolean(bytes[0] != 0x00)),
            ColumnType::Varchar => Ok(Expression::String(String::from_utf8(bytes.to_vec())
                .map_err(|e| MalformedQuery(String::from("Cannot parse from string")))?
            )),
            ColumnType::Date => todo!(),
            ColumnType::Blob => todo!(),
            ColumnType::Null => panic!("Invalid code path"),
        }
    }

    pub fn serialize(&self) -> Bytes {
        match self {
            Expression::String(string) => Bytes::copy_from_slice(string.as_bytes()),
            Expression::Boolean(boolean_value) => {
                if *boolean_value {
                    Bytes::from(vec![0x01])
                } else {
                    Bytes::from(vec![0x00])
                }
            }
            Expression::NumberF64(number_f64) => Bytes::copy_from_slice(&number_f64.to_le_bytes()),
            Expression::NumberI64(number_i64) => Bytes::copy_from_slice(&number_i64.to_le_bytes()),
            Expression::Null => Bytes::copy_from_slice(&vec![]),
            _ => panic!("")
        }
    }

    pub fn is_number(&self) -> bool {
        match self {
            Expression::NumberF64(_) |
            Expression::NumberI64(_) => true,
            _ => false
        }
    }

    pub fn get_f64(&self) -> Result<f64, SimpleDbError> {
        match self {
            Expression::Boolean(boolean) => {
                if *boolean {
                    Ok(1.0)
                } else {
                    Ok(0.0)
                }
            },
            Expression::NumberF64(number) => Ok(*number),
            Expression::NumberI64(number) => Ok(*number as f64),
            _ => Err(MalformedQuery(String::from("Cannot get F64 from expression")))
        }
    }

    pub fn identifier_eq(&self, expected_identifier: &str) -> bool {
        match self {
            Expression::Identifier(actual_identifier) => actual_identifier == expected_identifier,
            _ => false
        }
    }

    pub fn is_constant_expression(&self) -> bool {
        match self {
            Expression::None => panic!(""),
            Expression::Binary(_, left, right) => {
                left.is_constant_expression() && right.is_constant_expression()
            },
            Expression::Unary(_, expr) => expr.is_constant_expression(),
            Expression::Null |
            Expression::String(_) |
            Expression::Boolean(_) |
            Expression::NumberF64(_) |
            Expression::NumberI64(_) => true,
            Expression::Identifier(_) => false,
        }
    }

    pub fn is_constant(&self) -> bool {
        match self {
            Expression::Null |
            Expression::String(_) |
            Expression::Boolean(_) |
            Expression::NumberF64(_) |
            Expression::NumberI64(_) => true,
            _ => false
        }
    }

    pub fn add(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a + b)
    }

    pub fn subtract(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a - b)
    }

    pub fn multiply(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a * b)
    }

    pub fn divide(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a / b)
    }

    pub fn or(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        match (self, other) {
            (Expression::Boolean(a), Expression::Boolean(b)) => Ok(Expression::Boolean(*a || *b)),
            _ => Err(MalformedQuery(String::from("Cannot or values")))
        }
    }

    pub fn and(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        match (self, other) {
            (Expression::Null, _) |
            (_, Expression::Null) => Ok(Expression::Null),
            (Expression::Boolean(a), Expression::Boolean(b)) => Ok(Expression::Boolean(*a && *b)),
            _ => Err(MalformedQuery(String::from("Cannot and values")))
        }
    }

    pub fn greater(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Null,
            Expression::Null,
            |a, b| a > b,
            |a, b| a > b
        )
    }

    pub fn greater_equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Null,
            Expression::Null,
            |a, b| a >= b,
            |a, b| a >= b
        )
    }

    pub fn less(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Null,
            Expression::Null,
            |a, b| a < b,
            |a, b| a <= b
        )
    }

    pub fn less_equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Null,
            Expression::Null,
            |a, b| a <= b,
            |a, b| a <= b
        )
    }

    pub fn equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Boolean(true),
            Expression::Boolean(false),
            |a, b| a == b,
            |a, b| a == b
        )
    }

    pub fn not_equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Boolean(false),
            Expression::Boolean(true),
            |a, b| a != b,
            |a, b| a != b
        )
    }

    fn comparation_op(
        &self,
        other: &Expression,
        null_null_return_value: Expression,
        null_some_return_value: Expression,
        operation_f64: impl Fn(f64, f64) -> bool,
        operation_str: impl Fn(&str, &str) -> bool
    ) -> Result<Expression, SimpleDbError> {
        match (self, other) {
            (Expression::NumberI64(a), Expression::NumberI64(b)) => {
                Ok(Expression::Boolean(operation_f64(*a as f64, *b as f64)))
            }
            (Expression::NumberF64(a), Expression::NumberF64(b)) => {
                Ok(Expression::Boolean(operation_f64(*a, *b)))
            }
            (Expression::NumberF64(a), Expression::NumberI64(b)) => {
                Ok(Expression::Boolean(operation_f64(*a, *b as f64)))
            }
            (Expression::NumberI64(a), Expression::NumberF64(b)) => {
                Ok(Expression::Boolean(operation_f64(*a as f64, *b)))
            },
            (Expression::String(a), Expression::String(b)) => {
                Ok(Expression::Boolean(operation_str(a, b)))
            },
            (Expression::Null, Expression::Null) => {
                Ok(null_null_return_value)
            },
            (Expression::Null, _) |
            (_, Expression::Null) => {
                Ok(null_some_return_value)
            },
            _ => Err(MalformedQuery(String::from("Cannot compare values")))
        }
    }

    fn arithmetic_op<F>(&self, other: &Expression, op: F) -> Result<Expression, SimpleDbError>
    where
        F: Fn(f64, f64) -> f64
    {
        if matches!(other, Expression::Null) {
            return Ok(Expression::Null);
        }
        if !self.is_number() && !other.is_number() {
            return Err(MalformedQuery(String::from("")));
        }

        match (other, self) {
            (Expression::NumberI64(a), Expression::NumberI64(b)) => {
                Ok(Expression::NumberI64(op(*a as f64, *b as f64) as i64))
            }
            (Expression::NumberF64(a), Expression::NumberF64(b)) => {
                Ok(Expression::NumberF64(op(*a, *b)))
            }
            (Expression::NumberF64(a), Expression::NumberI64(b)) => {
                Ok(Expression::NumberF64(op(*a, *b as f64)))
            }
            (Expression::NumberI64(a), Expression::NumberF64(b)) => {
                Ok(Expression::NumberF64(op(*a as f64, *b)))
            }
            _ => Err(MalformedQuery(String::from("Cannot add values")))
        }
    }
}

impl BinaryOperator {
    //Takes booleans, Produces boolean
    pub fn is_logical(&self) -> bool {
        match self {
            BinaryOperator::And |
            BinaryOperator::Or => true,
            _ => false
        }
    }

    //Takes numbers, Produces boolean
    pub fn is_arithmetic(&self) -> bool {
        match self {
            BinaryOperator::Add |
            BinaryOperator::Subtract |
            BinaryOperator::Multiply |
            BinaryOperator::Divide => true,
            BinaryOperator::And |
            BinaryOperator::Or |
            BinaryOperator::NotEqual |
            BinaryOperator::Equal |
            BinaryOperator::Greater |
            BinaryOperator::GreaterEqual |
            BinaryOperator::Less |
            BinaryOperator::LessEqual => false
        }
    }

    //Takes comparable args, Produces boolean
    pub fn is_comparation(&self) -> bool {
        match self {
            BinaryOperator::Add |
            BinaryOperator::Subtract |
            BinaryOperator::Multiply |
            BinaryOperator::Divide => false,
            BinaryOperator::And |
            BinaryOperator::Or |
            BinaryOperator::NotEqual |
            BinaryOperator::Equal |
            BinaryOperator::Greater |
            BinaryOperator::GreaterEqual |
            BinaryOperator::Less |
            BinaryOperator::LessEqual => true
        }
    }
}