use shared::SimpleDbError;
use shared::SimpleDbError::IllegalToken;
use crate::ColumnType;
use crate::sql::statement::{CreateStatement, Statement};
use crate::sql::token::Token;
use crate::sql::tokenizer::Tokenizer;

pub struct Parser {
    tokenizer: Tokenizer,
}

impl Parser {
    pub fn create_parser(query: String) -> Parser {
        Parser {
            tokenizer: Tokenizer::create(query),
        }
    }

    pub fn parse(
        &mut self,
    ) -> Result<Statement, SimpleDbError> {
        match self.tokenizer.next_token()? {
            Token::Select => self.select(),
            Token::Update => self.update(),
            Token::Delete => self.delete(),
            Token::Insert => self.insert(),
            Token::Create => self.create_table(),
            Token::StartTransaction => Ok(Statement::StartTransaction),
            Token::Commit => Ok(Statement::Commit),
            Token::Rollback => Ok(Statement::Rollback),
            _ => Err(SimpleDbError::IllegalToken(self.tokenizer.current_location(), String::from("Unknown keyword")))
        }
    }

    fn select(&mut self) -> Result<Statement, SimpleDbError> {
        todo!()
    }

    fn update(&mut self) -> Result<Statement, SimpleDbError> {
        todo!()
    }

    fn insert(&mut self) -> Result<Statement, SimpleDbError> {
        todo!()
    }

    fn delete(&mut self) -> Result<Statement, SimpleDbError> {
        todo!()
    }

    fn create_table(&mut self) -> Result<Statement, SimpleDbError> {
        self.expect_token(Token::Table)?;
        let table_name_token = self.advance()?;
        match table_name_token {
            Token::Identifier(table_name) => {
                self.expect_token(Token::OpenParen)?;
                let columns = self.create_table_columns()?;

                Ok(Statement::CreateTable(CreateStatement{
                    table_name,
                    columns
                }))
            },
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Expect table name")))
        }
    }

    fn create_table_columns(&mut self) -> Result<Vec<(String, ColumnType, bool)>, SimpleDbError> {
        let mut columns = Vec::new();
        while !self.check(Token::CloseParen)? {
            let mut is_primary = false;

            if self.check(Token::Primary)? {
                self.expect_token(Token::Key)?;
                is_primary = true;
            }

            let table_name = self.identifier()?;
            let column_type = self.column_type()?;

            columns.push((table_name, column_type, is_primary));

            if !self.check(Token::CloseParen)? {
                self.expect_token(Token::Comma)?;
            }
        }

        Ok(columns)
    }

    fn column_type(&mut self) -> Result<ColumnType, SimpleDbError> {
        match self.advance()? {
            Token::ColumnType(column_type) => Ok(column_type),
            _ => Err(SimpleDbError::IllegalToken(self.tokenizer.current_location(), String::from("Expected column type")))
        }
    }

    fn identifier(&mut self) -> Result<String, SimpleDbError> {
        match self.advance()? {
            Token::Identifier(identifier) => Ok(identifier),
            _ => Err(SimpleDbError::IllegalToken(self.tokenizer.current_location(), String::from("Expected identifier")))
        }
    }

    fn advance(&mut self) -> Result<Token, SimpleDbError> {
        self.tokenizer.next_token()
    }

    fn check(&mut self, expected_token: Token) -> Result<bool, SimpleDbError> {
        let current_token = self.tokenizer.last_token();
        Ok(matches!(current_token.clone(), expected_token))
    }

    fn expect_token(&mut self, expected_token: Token) -> Result<Token, SimpleDbError> {
        let current_token = self.tokenizer.next_token()?;
        if matches!(current_token.clone(), expected_token) {
            Ok(current_token)
        } else {
            Err(SimpleDbError::IllegalToken(
                self.tokenizer.current_location(),
                format!("Expected token {:?} but found {:?}", expected_token, current_token))
            )
        }
    }
}