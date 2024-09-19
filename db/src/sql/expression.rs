#[derive(Debug, PartialEq)]
pub enum Expression {
    None,

    Binary(BinaryOperator, Box<Expression>, Box<Expression>),
    Unary(UnaryOperator, Box<Expression>),
    String(String),
    Identifier(String),
    Boolean(bool),
    NumberF64(f64),
    NumberI64(i64),
}

#[derive(Debug, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
}

#[derive(Debug, PartialEq)]
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