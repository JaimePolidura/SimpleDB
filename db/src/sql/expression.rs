use bytes::Bytes;
use shared::SimpleDbError;
use std::cmp::PartialEq;
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

    pub fn get_bytes(&self) -> Bytes {
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

    pub fn identifier_eq(&self, expected_identifier: &String) -> bool {
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
            Expression::String(_) |
            Expression::Boolean(_) |
            Expression::NumberF64(_) |
            Expression::NumberI64(_) => true,
            Expression::Identifier(_) => false,
        }
    }

    pub fn is_constant(&self) -> bool {
        match self {
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
            (Expression::Boolean(a), Expression::Boolean(b)) => Ok(Expression::Boolean(*a && *b)),
            _ => Err(MalformedQuery(String::from("Cannot and values")))
        }
    }

    pub fn greater(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(other, |a, b| a > b, |a, b| a > b)
    }

    pub fn greater_equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(other, |a, b| a >= b, |a, b| a >= b)
    }

    pub fn less(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(other, |a, b| a < b, |a, b| a <= b)
    }

    pub fn less_equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(other, |a, b| a <= b, |a, b| a <= b)
    }

    pub fn equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(other, |a, b| a == b, |a, b| a == b)
    }

    pub fn not_equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(other, |a, b| a != b, |a, b| a != b)
    }

    fn comparation_op(
        &self,
        other: &Expression,
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
            _ => Err(MalformedQuery(String::from("Cannot compare values")))
        }
    }

    fn arithmetic_op<F>(&self, other: &Expression, op: F) -> Result<Expression, SimpleDbError>
    where
        F: Fn(f64, f64) -> f64
    {
        if self.is_number() || other.is_number() {
            return Err(MalformedQuery(String::from("Cannot add values")));
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