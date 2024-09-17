use std::str::FromStr;
use shared::SimpleDbError::{MalformedNumber, MalformedString, IllegalToken};
use shared::TokenLocation;
use crate::ColumnType;
use crate::sql::token::{Token};

pub struct Tokenizer {
    string: String,
    //This will point to the next character to scan before calling next_token()
    next: usize,

    current_line: usize,
    current_column_index: usize,

    last_token: Option<Token>,
}

impl Tokenizer {
    pub fn create(
        string: String,
    ) -> Tokenizer {
        Tokenizer { string, next: 0, current_line: 1, current_column_index: 0, last_token: None }
    }

    pub fn last_token(&self) -> &Token {
        self.last_token.as_ref().unwrap()
    }

    pub fn next_token(&mut self) -> Result<Token, shared::SimpleDbError> {
        let next_token = self.get_token()?;
        self.last_token = Some(next_token.clone());
        Ok(next_token)
    }

    fn get_token(&mut self) -> Result<Token, shared::SimpleDbError> {
        self.skip_whitespaces();

        if self.end_reached() {
            return Ok(Token::EOF);
        }
        if self.is_number() {
            return self.number();
        }
        if self.is_alpha() {
            return self.identifier();
        }

        match self.advance() {
            '(' => Ok(Token::OpenParen),
            ')' => Ok(Token::CloseParen),
            ',' => Ok(Token::Comma),
            '+' => Ok(Token::Plus),
            '-' => Ok(Token::Minus),
            '*' => Ok(Token::Star),
            '/' => Ok(Token::Slash),
            ';' => Ok(Token::Seimicolon),
            '"' => self.string(),
            '>' => self.match_char_or('=', Token::GreaterEqual, Token::Greater),
            '<' => self.match_char_or('=', Token::LessEqual, Token::LessEqual),
            '=' => self.match_char_or('=', Token::EqualEqual, Token::Equal),
            '!' => self.match_char_or_error('=', Token::NotEqual),
            _ => Err(IllegalToken(self.current_location(), String::from("Unexpected token")))
        }
    }

    fn string(&mut self) -> Result<Token, shared::SimpleDbError> {
        let mut string = String::new();

        while !self.end_reached() && self.current() != '"' {
            string.push(self.advance());
        }

        if self.end_reached() || self.current() != '"' {
            return Err(MalformedString(self.next))
        }

        //Get rid of "
        self.advance();

        Ok(Token::String(string))
    }

    fn identifier(&mut self) -> Result<Token, shared::SimpleDbError> {
        match self.advance().to_uppercase().next().unwrap() {
            'A' => self.match_string_or_other_identifier("ND", Token::And, 1),
            'B' => {
                if self.advance_if_next_string_eq("OOLEAN") {
                    Ok(Token::ColumnType(ColumnType::BOOLEAN))
                } else if self.advance_if_next_string_eq("LOB") {
                    Ok(Token::ColumnType(ColumnType::BLOB))
                } else {
                    //Adjust, so that next points to the first char of the indentifier
                    self.next -= 1;
                    Ok(self.other_identifier())
                }
            },
            'K' => self.match_string_or_other_identifier("EY", Token::Key, 1),
            'P' => self.match_string_or_other_identifier("RIMARY", Token::Primary, 1),
            'O' => self.match_string_or_other_identifier("R", Token::Or, 1),
            'R' => {
                if self.advance_if_next_string_eq("OLLBACK") {
                    Ok(Token::Rollback)
                } else {
                    self.next -= 1;
                    Ok(self.other_identifier())
                }
            }
            'S' => {
                if self.advance_if_next_string_eq("ELECT") {
                    Ok(Token::Select)
                } else if self.advance_if_next_string_eq("TART_TRANSACTION") {
                    Ok(Token::StartTransaction)
                } else if self.advance_if_next_string_eq("ET") {
                    Ok(Token::Set)
                } else {
                    self.next -= 1;
                    Ok(self.other_identifier())
                }
            },
            'W' => self.match_string_or_other_identifier("HERE", Token::Where, 1),
            'F' => {
                if self.advance_if_next_string_eq("ROM") {
                    Ok(Token::From)
                } else if self.advance_if_next_string_eq("32") {
                    Ok(Token::ColumnType(ColumnType::F32))
                } else if self.advance_if_next_string_eq("64") {
                    Ok(Token::ColumnType(ColumnType::F64))
                } else {
                    self.next -= 1;
                    Ok(self.other_identifier())
                }
            },
            'C' => {
                if self.advance_if_next_string_eq("REATE") {
                    Ok(Token::Create)
                } else if self.advance_if_next_string_eq("OMMIT") {
                    Ok(Token::Commit)
                } else {
                    self.next -= 1;
                    Ok(self.other_identifier())
                }
            },
            'T' => {
                if self.advance_if_next_string_eq("ABLE") {
                    Ok(Token::Table)
                } else {
                    self.next -= 1;
                    Ok(self.other_identifier())
                }
            },
            'L' => self.match_string_or_other_identifier("IMIT", Token::Limit, 1),
            'U' => {
                if self.advance_if_next_string_eq("PDATE") {
                    Ok(Token::Update)
                } else if self.advance_if_next_string_eq("8") {
                    Ok(Token::ColumnType(ColumnType::U8))
                } else if self.advance_if_next_string_eq("16") {
                    Ok(Token::ColumnType(ColumnType::U16))
                } else if self.advance_if_next_string_eq("32") {
                    Ok(Token::ColumnType(ColumnType::U32))
                } else if self.advance_if_next_string_eq("64") {
                    Ok(Token::ColumnType(ColumnType::U64))
                } else {
                    self.next -= 1;
                    Ok(self.other_identifier())
                }
            },
            'D' => {
                if self.advance_if_next_string_eq("ATE") {
                    Ok(Token::ColumnType(ColumnType::DATE))
                } else if self.advance_if_next_string_eq("ELETE"){
                    Ok(Token::Delete)
                } else {
                    self.next -= 1;
                    Ok(self.other_identifier())
                }
            },
            'I' => {
                if self.advance_if_next_string_eq("NSERT") {
                    Ok(Token::Insert)
                }else if self.advance_if_next_string_eq("NTO") {
                    Ok(Token::Into)
                } else if self.advance_if_next_string_eq("8") {
                    Ok(Token::ColumnType(ColumnType::I8))
                } else if self.advance_if_next_string_eq("16") {
                    Ok(Token::ColumnType(ColumnType::I16))
                } else if self.advance_if_next_string_eq("32") {
                    Ok(Token::ColumnType(ColumnType::I32))
                } else if self.advance_if_next_string_eq("64") {
                    Ok(Token::ColumnType(ColumnType::I64))
                } else {
                    self.next -= 1;
                    Ok(self.other_identifier())
                }
            },
            'V' => {
                if self.advance_if_next_string_eq("ARCHAR") {
                    Ok(Token::ColumnType(ColumnType::VARCHAR))
                } else if self.advance_if_next_string_eq("ALUES") {
                    Ok(Token::Values)
                } else {
                    Ok(self.other_identifier())
                }
            },
            _ => {
                self.next -= 1;
                Ok(self.other_identifier())
            }
        }
    }

    fn other_identifier(&mut self) -> Token {
        let mut other_identifier = String::new();
        other_identifier.push(self.advance());

        while !self.end_reached() && (self.is_alpha() || self.is_number()) {
            other_identifier.push(self.advance());
        }

        Token::Identifier(other_identifier)
    }

    fn number(&mut self) -> Result<Token, shared::SimpleDbError> {
        let start_number_index = self.next;
        let mut has_decimals = false;

        //Not decimal number part
        while !self.end_reached() && self.is_number() {
            self.advance();
        }

        //Has decimals?
        if !self.end_reached() && (self.current() == '.') {
            has_decimals = true;

            //Decimal part
            self.advance();
            while !self.end_reached() && self.is_number() {
                self.advance();
            }
        }

        let number_string = &self.string[start_number_index..self.next];

        if has_decimals {
            match f64::from_str(number_string) {
                Ok(f64_value) => Ok(Token::NumberF64(f64_value)),
                Err(e) => Err(MalformedNumber(self.next - 1)),
            }
        } else {
            match number_string.parse::<i64>() {
                Ok(i64_value) => Ok(Token::NumberI64(i64_value)),
                Err(_) => Err(MalformedNumber(self.next - 1)),
            }
        }
    }

    fn skip_whitespaces(&mut self) {
        loop {
            if self.end_reached() {
                return;
            }

            match self.string.chars().nth(self.next).unwrap() {
                ' ' | '\t' | '\r' => {
                    self.current_column_index += 1;
                    self.next += 1;
                },
                '\n' => {
                    self.next += 1;
                    self.current_line += 1;
                    self.current_column_index = 0;
                }
                _ => break,
            }
        }
    }

    fn is_alpha(&self) -> bool {
        let char = self.current();
        char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z'
    }

    fn is_number(&self) -> bool {
        let char = self.current();
        char >= '0' && char <= '9'
    }

    fn match_char_or_error(
        &mut self,
        next: char,
        true_token: Token,
    ) -> Result<Token, shared::SimpleDbError> {
        if self.advance_if_next_char_eq(next) {
            Ok(true_token)
        } else {
            Err(IllegalToken(self.current_location(), String::from("Unknown token")))
        }
    }

    fn match_char_or(
        &mut self,
        next: char,
        true_token: Token,
        false_token: Token
    ) -> Result<Token, shared::SimpleDbError> {
        if self.advance_if_next_char_eq(next) {
            Ok(true_token)
        } else {
            Ok(false_token)
        }
    }

    fn match_string_or_other_identifier(
        &mut self,
        string_to_match: &str,
        true_token: Token,
        adjust_other_identifier: usize,
    ) -> Result<Token, shared::SimpleDbError> {
        let is_next_string = self.advance_if_next_string_eq(string_to_match);
        if is_next_string {
            Ok(true_token)
        } else {
            self.next -= adjust_other_identifier;
            self.current_column_index -= adjust_other_identifier;
            Ok(self.other_identifier())
        }
    }

    fn is_next_string_eq(
        &self,
        string_to_match: &str,
    ) -> bool {
        let start_string_index = self.next;
        let end_string_index = start_string_index + string_to_match.len();

        if end_string_index >= self.string.len() {
            return false;
        }

        let string_to_be_checked = self.string[start_string_index..end_string_index].to_string();
        string_to_be_checked.to_uppercase() == string_to_match
    }

    fn advance_if_next_string_eq(
        &mut self,
        string_to_match: &str,
    ) -> bool {
        if self.is_next_string_eq(string_to_match) {
            self.current_column_index += string_to_match.len();
            self.next += string_to_match.len();
            true
        } else {
            false
        }
    }

    fn advance_if_next_char_eq(&mut self, expected: char) -> bool {
        if self.current() == expected {
            self.advance();
            true
        } else {
            false
        }
    }

    fn advance_backward(&mut self) {
        self.current_column_index -= 1;
        self.next -= 1;
    }

    fn advance(&mut self) -> char {
        let current = self.current();
        self.current_column_index += 1;
        self.next += 1;
        current
    }

    fn current(&self) -> char {
        self.string.chars().nth(self.next).unwrap()
    }

    fn end_reached(&self) -> bool {
        self.next >= self.string.len()
    }

    fn char_at(&self, index: usize) -> char {
        self.string.chars().nth(index).unwrap()
    }

    pub fn current_location(&self) -> TokenLocation {
        TokenLocation {
            column_index: self.current_column_index,
            line: self.current_line,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::ColumnType;
    use crate::sql::token::Token;
    use crate::sql::tokenizer::Tokenizer;

    #[test]
    fn select() {
        let mut tokenizer = Tokenizer::create(String::from(
            "SELECT * FROM personas WHERE nombre = \"Jaime\" AND dinero >= 100.2 LIMIT 10"
        ));
        let personas = String::from("personas");
        let nombre = String::from("nombre");
        let dinero = String::from("dinero");
        let jaime = String::from("Jaime");

        assert!(matches!(tokenizer.get_token().unwrap(), Token::Select));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Star));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::From));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(personas)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Where));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(nombre)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Equal));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::String(jaime)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::And));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(dinero)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::GreaterEqual));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::NumberF64(100.2)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Limit));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::NumberI64(10)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::EOF));
    }

    #[test]
    fn insert() {
        let mut tokenizer = Tokenizer::create(String::from(
            "INSERT INTO personas (id, nombre, dinero) VALUES (1, \"Jaime\", 100);"
        ));
        let personas = String::from("personas");
        let nombre = String::from("nombre");
        let dinero = String::from("dinero");
        let jaime = String::from("Jaime");
        let id = String::from("id");

        assert!(matches!(tokenizer.get_token().unwrap(), Token::Insert));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Into));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(personas)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::OpenParen));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(id)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Comma));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(nombre)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Comma));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(dinero)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::CloseParen));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Values));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::OpenParen));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::NumberI64(1)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Comma));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::String(jaime)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Comma));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::NumberI64(100)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::CloseParen));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Seimicolon));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::EOF));
    }

    #[test]
    fn delete() {
        let mut tokenizer = Tokenizer::create(String::from(
            "DELETE FROM personas WHERE id = 1"
        ));
        let personas = String::from("personas");
        let dinero = String::from("dinero");
        let id = String::from("id");

        assert!(matches!(tokenizer.get_token().unwrap(), Token::Delete));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::From));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(personas)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Where));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(id)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Equal));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::NumberI64(1)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::EOF));
    }

    #[test]
    fn update() {
        let mut tokenizer = Tokenizer::create(String::from(
            "UPDATE personas SET dinero = 101 WHERE id = 1"
        ));
        let personas = String::from("personas");
        let dinero = String::from("dinero");
        let id = String::from("id");

        assert!(matches!(tokenizer.get_token().unwrap(), Token::Update));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(personas)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Set));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(dinero)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Equal));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::NumberI64(101)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Where));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(id)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Equal));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::NumberI64(1)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::EOF));
    }

    #[test]
    fn create_table() {
        let mut tokenizer = Tokenizer::create(String::from(
            "CREATE TABLE personas (
                id u64 PRIMARY KEY,
                nombre VARCHAR,
                dinero f32
              );"
        ));
        let personas = String::from("personas");
        let nombre = String::from("nombre");
        let dinero = String::from("dinero");
        let jaime = String::from("Jaime");
        let id = String::from("id");

        assert!(matches!(tokenizer.get_token().unwrap(), Token::Create));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Table));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(personas)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::OpenParen));

        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(id)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::ColumnType(ColumnType::U64)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Primary));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Key));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Comma));

        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(nombre)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::ColumnType(ColumnType::VARCHAR)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Comma));

        assert!(matches!(tokenizer.get_token().unwrap(), Token::Identifier(dinero)));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::ColumnType(ColumnType::F32)));

        assert!(matches!(tokenizer.get_token().unwrap(), Token::CloseParen));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::Seimicolon));
        assert!(matches!(tokenizer.get_token().unwrap(), Token::EOF));
    }
}