use bytes::{BufMut, Bytes};
use crate::table::column_type::ColumnType;

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

    Identifier(String), //Ohter identifier, like table or column names
    String(String), // "some text"
    NumberI64(i64), // any number
    NumberF64(f64), // any number
    ColumnType(ColumnType),

    EOF
}

impl Token {
    pub fn convert_to_bytes(&self) -> Result<bytes::Bytes, ()> {
        match self {
            Token::String(string) => Ok(bytes::Bytes::from(string.as_bytes().to_vec())),
            Token::NumberI64(number) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.put_i64_le(*number);
                Ok(bytes::Bytes::from(bytes))
            },
            Token::True => Ok(bytes::Bytes::from(vec![0x01])),
            Token::False => Ok(bytes::Bytes::from(vec![0x00])),
            Token::NumberF64(number) => {
                let mut bytes: Vec<u8> = Vec::new();
                bytes.put_f64_le(*number);
                Ok(bytes::Bytes::from(bytes))
            },
            Token::Null => Ok(Bytes::new()),
            _ => Err(()) //Cannot cast to bytes
        }
    }

    pub fn can_be_converted_to_bytes(&self) -> bool {
        match self {
            Token::String(_) | Token::NumberI64(_) | Token::True | Token::False | Token::NumberF64(_) => true,
            _ => false
        }
    }
}