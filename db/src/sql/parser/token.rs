use shared::{Type, Value};

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
    Explain,

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
            Token::String(string) => Ok(Value::create_string(string.clone())),
            Token::NumberI64(number) => Ok(Value::create_i64(*number)),
            Token::True => Ok(Value::create_boolean(true)),
            Token::False => Ok(Value::create_boolean(false)),
            Token::NumberF64(number) => Ok(Value::create_f64(*number)),
            Token::Null => Ok(Value::create_null()),
            _ => Err(()) //Cannot cast to bytes
        }
    }
}