use db::ColumnType;
use shared::SimpleDbError;
use shared::SimpleDbError::{MalformedNumber, MalformedString, UnexpectedToken};

pub enum Token {
    OpenParen, // "("
    CloseParen, // ")"
    Comma, // ","
    Plus, // "+"
    Star, // "*"
    Minus, // "-"
    Slash, // "/"
    Less, // "<"
    Equal, // "=="
    Greater, // ">"
    GreaterEqual, // ">="
    LessEqual, // "<="
    NotEqual, // "!="

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

    Identifier(String), //Ohter identifier, like table or column names
    String(String), // "some text"
    NumberI64(i64), // any number
    NumberF64(f64), // any number
    ColumnType(db::ColumnType),

    EOF
}

pub struct Toknizer {
    string: String,
    current_position: usize,
}

impl Toknizer {
    pub fn create(
        string: String,
    ) -> Toknizer {
        Toknizer { string, current_position: 0 }
    }

    pub fn next_token(&mut self) -> Result<Token, shared::SimpleDbError> {
        self.skip_whitespaces();

        if self.current_position >= self.string.len() {
            return Ok(Token::EOF);
        }

        if self.is_number() {
            return self.number();
        }
        if self.is_alpha() {
            return self.identifier();
        }

        match self.string.chars().nth(self.current_position).unwrap() {
            '(' => Ok(Token::OpenParen),
            ')' => Ok(Token::CloseParen),
            ',' => Ok(Token::Comma),
            '+' => Ok(Token::Plus),
            '-' => Ok(Token::Minus),
            '*' => Ok(Token::Star),
            '/' => Ok(Token::Slash),
            '"' => self.string(),
            '>' => self.try_match_one_char('=', Token::GreaterEqual, Token::Greater),
            '<' => self.try_match_one_char('=', Token::LessEqual, Token::LessEqual),
            '=' => self.match_one_char('=', Token::Equal),
            '!' => self.match_one_char('=', Token::NotEqual),
            _ => Err(shared::SimpleDbError::UnexpectedToken(self.string.clone(), self.current_position))
        }
    }

    fn string(&mut self) -> Result<Token, shared::SimpleDbError> {
        let mut string = String::new();

        self.current_position += 1;
        while self.current_position < self.string.len() && self.string[self.current_position] != '"' {
            self.current_position += 1;
            string.push(self.string[self.current_position]);
        }

        if self.string[self.current_position] != '"' {
            return Err(MalformedString(self.current_position))
        }

        self.current_position += 1;

        Ok(Token::String(string))
    }

    fn identifier(&mut self) -> Result<Token, shared::SimpleDbError> {
        match self.string.chars().nth(self.current_position).unwrap().to_uppercase().next().unwrap() {
            'A' => self.try_match_string("ND", Token::And, self.other_identifier()),
            'B' => {
                if self.is_next_string("OOLEAN") {
                    Ok(Token::ColumnType(ColumnType::BOOLEAN))
                } else if self.is_next_string("LOB") {
                    Ok(Token::ColumnType(ColumnType::BLOB))
                } else {
                    Ok(self.other_identifier())
                }
            }
            'O' => self.try_match_string("R", Token::Or, self.other_identifier()),
            'S' => self.try_match_string("ELECT", Token::Select, self.other_identifier()),
            'W' => self.try_match_string("HERE", Token::Where, self.other_identifier()),
            'F' => {
                if self.is_next_string("ROM") {
                    Ok(Token::From)
                } else if self.is_next_string("32") {
                    Ok(Token::ColumnType(ColumnType::F32))
                } else if self.is_next_string("64") {
                    Ok(Token::ColumnType(ColumnType::F64))
                } else {
                    Ok(self.other_identifier())
                }
            },
            'C' => self.try_match_string("REATE", Token::Create, self.other_identifier()),
            'T' => {
                if self.is_next_string("ABLE") {
                    Ok(Token::Table)
                } else {
                    Ok(self.other_identifier())
                }
            },
            'L' => self.try_match_string("IMIT", Token::Limit, self.other_identifier()),
            'U' => {
                if self.is_next_string("PDATE") {
                    Ok(Token::Update)
                } else if self.is_next_string("8") {
                    Ok(Token::ColumnType(ColumnType::U8))
                } else if self.is_next_string("16") {
                    Ok(Token::ColumnType(ColumnType::U16))
                } else if self.is_next_string("32") {
                    Ok(Token::ColumnType(ColumnType::U32))
                } else if self.is_next_string("64") {
                    Ok(Token::ColumnType(ColumnType::U64))
                } else {
                    Ok(self.other_identifier())
                }
            },
            'D' => {
                if self.is_next_string("ATE") {
                    Ok(Token::ColumnType(ColumnType::DATE))
                } else {
                    Ok(self.other_identifier())
                }
            },
            'I' => {
                if self.is_next_string("NSERT") {
                    Ok(Token::Insert)
                } else if self.is_next_string("8") {
                    Ok(Token::ColumnType(ColumnType::I8))
                } else if self.is_next_string("16") {
                    Ok(Token::ColumnType(ColumnType::I16))
                } else if self.is_next_string("32") {
                    Ok(Token::ColumnType(ColumnType::I32))
                } else if self.is_next_string("64") {
                    Ok(Token::ColumnType(ColumnType::I64))
                } else {
                    Ok(self.other_identifier())
                }
            },
            'V' => self.try_match_string("ALUES", Token::Values, self.other_identifier()),
            _ => Ok(self.other_identifier())
        }
    }

    fn other_identifier(&mut self) -> Token {
        let mut other_identifier = String::new();
        other_identifier.push(self.string[self.current_position]);
        self.current_position += 1;

        while self.current_position < self.string.len() && self.string[self.current_position] != ' ' {
            other_identifier.push(self.string[self.current_position]);
            self.current_position += 1;
        }

        Token::Identifier(other_identifier)
    }

    fn number(&mut self) -> Result<Token, shared::SimpleDbError> {
        let start_number_index = self.current_position;
        let mut has_decimals = false;

        //Not decimal number part
        self.current_position += 1;
        while self.current_position < self.string.len() && self.is_number() {
            self.current_position += 1;
        }

        //Has deco
        if self.string[self.current_position] == '.' || self.string[self.current_position] == ',' {
            has_decimals = true;

            //Decimal part
            self.current_position += 1;
            while self.current_position < self.string.len() && self.is_number() {
                self.current_position += 1;
            }
        }

        self.current_position += 1;
        let number_string = &self.string[start_number_index..self.current_position];

        if has_decimals {
            match number_string.parse::<f64>() {
                Ok(f64_value) => Ok(Token::NumberF64(f64_value)),
                Err(_) => return Err(MalformedNumber(self.current_position)),
            }
        } else {
            match number_string.parse::<i64>() {
                Ok(i64_value) => Ok(Token::NumberI64(i64_value)),
                Err(_) => return Err(MalformedNumber(self.current_position)),
            }
        }
    }

    fn skip_whitespaces(&mut self) {
        loop {
            self.current_position += 1;

            if self.current_position >= self.string.len() {
                return;
            }

            match self.string.chars().nth(self.current_position).unwrap() {
                ' ' | '\t' | '\n' | '\r' => continue,
                _ => break,
            }
        }
    }

    fn is_alpha(&self) -> bool {
        let char = self.string.chars().nth(self.current_position).unwrap();
        char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z'
    }

    fn is_number(&self) -> bool {
        let char = self.string.chars().nth(self.current_position).unwrap();
        char >= '0' && char <= '9'
    }

    fn match_one_char(
        &mut self,
        next: char,
        true_token: Token,
    ) -> Result<Token, shared::SimpleDbError> {
        let current_token = self.string[self.current_position + 1];
        if current_token == next {
            self.current_position += 1;
            Ok(true_token)
        } else {
           Err(UnexpectedToken(current_token, self.current_position))
        }
    }

    fn try_match_one_char(
        &mut self,
        next: char,
        true_token: Token,
        false_token: Token
    ) -> Result<Token, shared::SimpleDbError> {
        let current_token = self.string[self.current_position + 1];
        if current_token == next {
            self.current_position += 1;
            Ok(true_token)
        } else {
            Ok(false_token)
        }
    }

    fn try_match_string(
        &mut self,
        string_to_match: &str,
        true_token: Token,
        false_token: Token
    ) -> Result<Token, shared::SimpleDbError> {
        let is_next_string = self.is_next_string(string_to_match);
        if is_next_string {
            self.current_position += string_to_match.len();
            Ok(true_token)
        } else {
            Ok(false_token)
        }
    }

    fn is_next_string(
        &self,
        string_to_match: &str,
    ) -> bool {
        let start_string_index = self.current_position + 1;
        let end_string_index = start_string_index + string_to_match.len();

        if end_string_index >= self.string.len() {
            return false;
        }

        let string_to_be_checked = self.string[start_string_index..end_string_index].to_string();
        string_to_be_checked.to_uppercase() == string_to_match
    }
}