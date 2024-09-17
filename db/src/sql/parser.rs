use std::cmp::PartialEq;
use shared::SimpleDbError;
use shared::SimpleDbError::IllegalToken;
use crate::ColumnType;
use crate::sql::statement::{CreateTableStatement, Statement};
use crate::sql::token::Token;
use crate::sql::tokenizer::Tokenizer;

pub struct Parser {
    tokenizer: Tokenizer,
}

impl Parser {
    pub fn create(query: String) -> Parser {
        Parser {
            tokenizer: Tokenizer::create(query),
        }
    }

    pub fn next_statement(
        &mut self,
    ) -> Result<Statement, SimpleDbError> {
        let query = match self.tokenizer.next_token()? {
            Token::Select => self.select(),
            Token::Update => self.update(),
            Token::Delete => self.delete(),
            Token::Insert => self.insert(),
            Token::Create => self.create_table(),
            Token::StartTransaction => Ok(Statement::StartTransaction),
            Token::Commit => Ok(Statement::Commit),
            Token::Rollback => Ok(Statement::Rollback),
            _ => Err(SimpleDbError::IllegalToken(self.tokenizer.current_location(), String::from("Unknown keyword")))
        }?;
        self.expect_token(Token::Seimicolon)?;
        Ok(query)
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
        self.advance()?;
        self.expect_token(Token::Table)?;

        //Table name token
        match self.advance()? {
            Token::Identifier(table_name) => {
                self.expect_token(Token::OpenParen)?;
                let columns = self.create_table_columns()?;

                Ok(Statement::CreateTable(CreateTableStatement {
                    table_name,
                    columns
                }))
            },
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Expect table name")))
        }
    }

    fn create_table_columns(&mut self) -> Result<Vec<(String, ColumnType, bool)>, SimpleDbError> {
        let mut columns = Vec::new();
        let mut finished = false;

        while !finished {
            let table_name = self.identifier()?;
            let is_primary = self.is_primary_key()?;
            let column_type = self.column_type()?;

            columns.push((table_name, column_type, is_primary));

            if !self.maybe_expect_token(Token::CloseParen)? {
                self.expect_token(Token::Comma)?;
            } else {
                finished = true;
            }
        }

        Ok(columns)
    }

    fn is_primary_key(&mut self) -> Result<bool, SimpleDbError> {
        let mut is_primary = false;

        if self.maybe_expect_token(Token::Primary)? {
            self.expect_token(Token::Key)?;
            is_primary = true;
        }

        Ok(is_primary)
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
        let last_token = self.tokenizer.last_token().clone();
        self.tokenizer.next_token()?;
        Ok(last_token)
    }

    fn maybe_expect_token(&mut self, expected_token: Token) -> Result<bool, SimpleDbError> {
        let current_token = self.tokenizer.last_token().clone();
        if current_token == expected_token {
            self.advance()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn expect_token(&mut self, expected_token: Token) -> Result<Token, SimpleDbError> {
        let current_token = self.tokenizer.last_token().clone();
        if current_token == expected_token {
            self.advance()?;
            Ok(current_token)
        } else {
            Err(SimpleDbError::IllegalToken(
                self.tokenizer.current_location(),
                format!("Expected token {:?} but found {:?}", expected_token, current_token))
            )
        }
    }
}

#[cfg(test)]
mod test {
    use crate::ColumnType;
    use crate::sql::parser::Parser;
    use crate::sql::statement::Statement;

    #[test]
    fn create_table() {
        let mut parser = Parser::create(String::from(
            r#"CREATE TABLE personas (
                id PRIMARY KEY i64,
                nombre VARCHAR,
                dinero f64
               );"#
        ));
        let statement = parser.next_statement().unwrap();

        assert!(matches!(statement, Statement::CreateTable(_)));
        match statement {
            Statement::CreateTable(createStatement) => {
                assert_eq!(createStatement.table_name, String::from("personas"));
                assert_eq!(createStatement.columns.len(), 3);
                assert_eq!(createStatement.columns[0], (String::from("id"), ColumnType::I64, true));
                assert_eq!(createStatement.columns[1], (String::from("nombre"), ColumnType::VARCHAR, false));
                assert_eq!(createStatement.columns[2], (String::from("dinero"), ColumnType::F64, false));
            },
            _ => panic!()
        }
    }
}