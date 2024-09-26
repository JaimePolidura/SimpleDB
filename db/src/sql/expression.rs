use bytes::Bytes;
use shared::{utils, SimpleDbError};
use std::cmp::PartialEq;
use SimpleDbError::MalformedQuery;
use crate::value::{Type, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    None,
    Binary(BinaryOperator, Box<Expression>, Box<Expression>),
    Unary(UnaryOperator, Box<Expression>),
    Identifier(String),
    Literal(Value),
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
    pub fn is_null(&self) -> bool {
        match &self {
            Expression::Literal(value) => value.is_null(),
            _ => false
        }
    }

    pub fn serialize(&self) -> Bytes {
        match self {
            Expression::Literal(value) => value.serialize(),
            _ => panic!("")
        }
    }

    pub fn is_number(&self) -> bool {
        match self {
            Expression::Literal(value) => value.is_number(),
            _ => false
        }
    }

    pub fn get_boolean(&self) -> Result<bool, SimpleDbError> {
        match self {
            Expression::Literal(value) => value.get_boolean()
                .map_err(|_| MalformedQuery(String::from("Cannot get boolean from expression"))),
            _ => Err(MalformedQuery(String::from("Cannot get F64 from expression")))
        }
    }

    pub fn get_f64(&self) -> Result<f64, SimpleDbError> {
        match self {
            Expression::Literal(value) => value.get_f64()
                .map_err(|_| MalformedQuery(String::from("Cannot get F64 from expression"))),
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
            Expression::Literal(_) => true,
            Expression::Identifier(_) => false,
        }
    }

    pub fn is_constant(&self) -> bool {
        matches!(self, Expression::Literal(_))
    }

    pub fn add(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a.add(b))
    }

    pub fn multiply(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a.multiply(b))
    }

    pub fn substract(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a.substract(b))
    }

    pub fn divide(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a.divide(b))
    }

    pub fn or(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        let value_self = self.get_value()?;
        let value_other = other.get_value()?;
        Ok(Expression::Literal(value_self.or(&value_other)?))
    }

    pub fn and(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        if self.is_null() || other.is_null() {
            return Ok(Expression::Literal(Value::Null));
        }

        let value_self = self.get_value()?;
        let value_other = other.get_value()?;
        Ok(Expression::Literal(value_self.and(&value_other)?))
    }

    pub fn greater(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::Null),
            Expression::Literal(Value::Null),
            |a, b| a.greater(b)
        )
    }

    pub fn greater_equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::Null),
            Expression::Literal(Value::Null),
            |a, b| a.greater_equal(b)
        )
    }

    pub fn less(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::Null),
            Expression::Literal(Value::Null),
            |a, b| a.less(b)
        )
    }

    pub fn less_equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::Null),
            Expression::Literal(Value::Null),
            |a, b| a.less_equal(b),
        )
    }

    pub fn equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::Boolean(true)),
            Expression::Literal(Value::Boolean(false)),
            |a, b| a.equal(b),
        )
    }

    pub fn not_equal(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::Boolean(false)),
            Expression::Literal(Value::Boolean(true)),
            |a, b| a.not_equal(b),
        )
    }

    pub fn get_value(&self) -> Result<Value, SimpleDbError> {
        match self {
            Expression::Literal(value) => Ok(value.clone()),
            _ => Err(MalformedQuery(String::from("Cannot get value from expression")))
        }
    }

    fn comparation_op<Op>(
        &self,
        other: &Expression,
        null_null_return_value: Expression,
        null_some_return_value: Expression,
        op: Op
    ) -> Result<Expression, SimpleDbError>
    where
        Op: Fn(&Value, &Value) -> Result<Value, SimpleDbError>
    {
        if self.is_null() && other.is_null() {
            return Ok(null_null_return_value);
        }
        if self.is_null() || other.is_null() {
            return Ok(null_some_return_value);
        }

        match &self {
            Expression::Literal(value) => Ok(
                Expression::Literal(op(value, &self.get_value()?)?)
            ),
            _ => Err(MalformedQuery(String::from("Cannot add values")))
        }
    }

    fn arithmetic_op<Op>(&self, other: &Expression, op: Op) -> Result<Expression, SimpleDbError>
    where
        Op: Fn(&Value, &Value) -> Result<Value, SimpleDbError>
    {
        if self.is_null() || other.is_null() {
            return Ok(Expression::Literal(Value::Null))
        }

        match &self {
            Expression::Literal(value) => Ok(
                Expression::Literal(op(value, &self.get_value()?)?)
            ),
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