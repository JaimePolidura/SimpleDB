use crate::value::{Type, Value};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    OpenParen, // "("
    CloseParen, // ")"
    Comma, // ","
    Plus, // "+"
    Star, // "*"
    Minus, // "-"
    Slash, // "/"
    Less, // "<"
    Equal, // "="
    EqualEqual, // "=="
    Greater, // ">"
    GreaterEqual, // ">="
    LessEqual, // "<="
    NotEqual, // "!="
    Semicolon,

    True, //15
    False,
    Null,
    And,
    Or,
    Select,
    Where,
    From,
    Create,
    Table,
    Limit,
    Update,
    Delete,
    Insert,
    Values,
    Into,
    Set,
    Primary,
    Key,
    StartTransaction, // "START_TRANSACTION"
    Rollback, // "ROLLBACK"
    Commit, // "COMMIT"
    Database,
    Show,
    Tables,
    Databases,
    Describe,
    Index,
    Async,
    On,

    Identifier(String), //Ohter identifier, like table or column names
    ColumnType(Type),
    String(String), // "some text"
    NumberI64(i64), // any number
    NumberF64(f64), // any number

    EOF
}

impl Token {
    pub fn serialize(&self) -> Result<Value, ()> {
        match self {
            Token::String(string) => Ok(Value::String(string.clone())),
            Token::NumberI64(number) => Ok(Value::I64(*number)),
            Token::True => Ok(Value::Boolean(true)),
            Token::False => Ok(Value::Boolean(false)),
            Token::NumberF64(number) => Ok(Value::F64(*number)),
            Token::Null => Ok(Value::Null),
            _ => Err(()) //Cannot cast to bytes
        }
    }
}