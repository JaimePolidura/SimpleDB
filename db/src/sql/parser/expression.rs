use bytes::Bytes;
use shared::{SimpleDbError, Value};
use std::cmp::PartialEq;
use std::collections::HashSet;
use SimpleDbError::MalformedQuery;

#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
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
    pub fn get_identifiers(&self) -> Vec<String> {
        let columns = self.get_identifiers_recursive();
        columns.into_iter().collect()
    }

    pub fn get_identifiers_recursive(&self) -> HashSet<String> {
        //Hashset to avoid duplicates
        let mut columns = HashSet::new();

        match self {
            Expression::Binary(_, left, right) => {
                columns.extend(left.get_identifiers());
                columns.extend(right.get_identifiers());
            },
            Expression::Unary(_, expr) => columns.extend(expr.get_identifiers()),
            Expression::Identifier(column_name) => { columns.insert(column_name.clone()); },
            Expression::Literal(_) => {}
        };

        columns
    }

    pub fn is_null(&self) -> bool {
        match &self {
            Expression::Literal(value) => value.is_null(),
            _ => false
        }
    }

    pub fn get_f64(&self) -> Result<f64, SimpleDbError> {
        match self {
            Expression::Literal(value) => value.get_f64()
                .map_err(|_| MalformedQuery(String::from("Cannot get F64 from expression"))),
            _ => Err(MalformedQuery(String::from("Cannot get F64 from expression")))
        }
    }

    pub fn get_literal_bytes(&self) -> Bytes {
        match self {
            Expression::Literal(value) => value.get_bytes().clone(),
            _ => panic!("")
        }
    }

    pub fn get_boolean(&self) -> Result<bool, SimpleDbError> {
        match self {
            Expression::Literal(value) => value.get_boolean()
                .map_err(|_| MalformedQuery(String::from("Cannot get boolean from expression"))),
            _ => Err(MalformedQuery(String::from("Cannot get F64 from expression")))
        }
    }

    pub fn get_identifier(&self) -> Result<String, SimpleDbError> {
        match self {
            Expression::Identifier(actual_identifier) => Ok(actual_identifier.clone()),
            _ => Err(MalformedQuery(String::from("Cannot get identifier from expression"))),
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
            Expression::Binary(_, left, right) => {
                left.is_constant_expression() && right.is_constant_expression()
            },
            Expression::Unary(_, expr) => expr.is_constant_expression(),
            Expression::Literal(_) => true,
            Expression::Identifier(_) => false,
        }
    }

    pub fn is_literal(&self) -> bool {
        matches!(self, Expression::Literal(_))
    }

    pub fn add(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a.add(b))
    }

    pub fn multiply(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a.multiply(b))
    }

    pub fn subtract(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.arithmetic_op(other, |a, b| a.subtract(b))
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
            return Ok(Expression::Literal(Value::create_null()));
        }

        let value_self = self.get_value()?;
        let value_other = other.get_value()?;
        Ok(Expression::Literal(value_self.and(&value_other)?))
    }

    pub fn gt(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::create_null()),
            Expression::Literal(Value::create_null()),
            |a, b| a.gt(b)
        )
    }

    pub fn ge(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::create_null()),
            Expression::Literal(Value::create_null()),
            |a, b| a.ge(b)
        )
    }

    pub fn lt(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::create_null()),
            Expression::Literal(Value::create_null()),
            |a, b| a.lt(b)
        )
    }

    pub fn le(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::create_null()),
            Expression::Literal(Value::create_null()),
            |a, b| a.le(b),
        )
    }

    pub fn eq(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::create_boolean(true)),
            Expression::Literal(Value::create_boolean(false)),
            |a, b| a.eq(b),
        )
    }

    pub fn ne(&self, other: &Expression) -> Result<Expression, SimpleDbError> {
        self.comparation_op(
            other,
            Expression::Literal(Value::create_boolean(false)),
            Expression::Literal(Value::create_boolean(true)),
            |a, b| a.ne(b),
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
        Op: Fn(&Value, &Value) -> bool
    {
        if self.is_null() && other.is_null() {
            return Ok(null_null_return_value);
        }
        if self.is_null() || other.is_null() {
            return Ok(null_some_return_value);
        }

        let other_value = other.get_value()?;
        let self_value = self.get_value()?;
        if !self_value.is_comparable(&other_value) {
            return Err(SimpleDbError::IllegalTypeOperation("Cannot compare values"));
        }

        match &self {
            Expression::Literal(value) => Ok(
                Expression::Literal(Value::create_boolean(op(&self_value, &other_value)))
            ),
            _ => Err(MalformedQuery(String::from("Cannot add values")))
        }
    }

    fn arithmetic_op<Op>(&self, other: &Expression, op: Op) -> Result<Expression, SimpleDbError>
    where
        Op: Fn(&Value, &Value) -> Result<Value, SimpleDbError>
    {
        if self.is_null() || other.is_null() {
            return Ok(Expression::Literal(Value::create_null()))
        }

        match &self {
            Expression::Literal(value) => Ok(
                Expression::Literal(op(value, &other.get_value()?)?)
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