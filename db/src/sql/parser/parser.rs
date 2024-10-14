use crate::selection::Selection;
use crate::sql::expression::{BinaryOperator, Expression, UnaryOperator};
use crate::sql::parser::token::Token;
use crate::sql::parser::tokenizer::Tokenizer;
use crate::sql::statement::{CreateTableStatement, DeleteStatement, InsertStatement, Limit, SelectStatement, Statement, UpdateStatement};
use shared::{SimpleDbError, Type, Value};
use shared::SimpleDbError::IllegalToken;
use crate::CreateIndexStatement;

const MAX_PRECEDENCE: u8 = u8::MAX;

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
    ) -> Result<Option<Statement>, SimpleDbError> {
        let mut first_token = self.tokenizer.next_token()?;
        let is_explain = matches!(first_token, Token::Explain);
        if is_explain {
            first_token = self.tokenizer.next_token()?;
        }

        let query = match first_token {
            Token::Select => self.select(is_explain),
            Token::Update => self.update(),
            Token::Delete => self.delete(),
            Token::Insert => self.insert(),
            Token::StartTransaction => self.start_transaction(),
            Token::Create => self.create_some(),
            Token::Rollback => self.rollback(),
            Token::Describe => self.describe(),
            Token::Commit => self.commit(),
            Token::Show => self.show(),
            Token::EOF => return Ok(None),
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Unknown keyword")))
        }?;
        self.expect_token(Token::Semicolon)?;
        Ok(Some(query))
    }

    //Only used for testing
    pub(crate) fn parse_expression(&mut self) -> Result<Expression, SimpleDbError> {
        self.tokenizer.next_token()?;
        self.expression(0)
    }

    fn select(&mut self, explain: bool) -> Result<Statement, SimpleDbError> {
        self.advance()?;
        let selection = self.selection()?;
        self.expect_token(Token::From)?;
        let table_name = self.identifier()?;
        let mut limit = Limit::None;
        let mut expression = None;

        if self.maybe_expect_token(Token::Limit)? {
            limit = self.limit()?;
        }
        if self.maybe_expect_token(Token::Where)? {
            expression = Some(self.expression(0)?);
        }
        if self.maybe_expect_token(Token::Limit)? {
            limit = self.limit()?;
        }

        Ok(Statement::Select(SelectStatement {
            where_expr: expression,
            table_name,
            selection,
            explain,
            limit
        }))
    }

    fn expression(&mut self, precedence: u8) -> Result<Expression, SimpleDbError> {
        let mut expression = self.parse_prefix()?;
        let mut next_precedence = self.get_precedence(self.tokenizer.last_token());

        while precedence < next_precedence {
            expression = self.parse_infix(next_precedence, expression)?;
            next_precedence = self.get_precedence(self.tokenizer.last_token());
        }

        Ok(expression)
    }

    fn parse_infix(&mut self, precedence: u8, left: Expression) -> Result<Expression, SimpleDbError> {
        let binary_operator = match self.advance()? {
            Token::And => BinaryOperator::And,
            Token::Or => BinaryOperator::Or,
            Token::NotEqual => BinaryOperator::NotEqual,
            Token::EqualEqual => BinaryOperator::Equal,
            Token::Less => BinaryOperator::Less,
            Token::LessEqual => BinaryOperator::LessEqual,
            Token::Greater => BinaryOperator::Greater,
            Token::GreaterEqual => BinaryOperator::GreaterEqual,
            Token::Plus => BinaryOperator::Add,
            Token::Slash => BinaryOperator::Divide,
            Token::Star => BinaryOperator::Multiply,
            Token::Minus => BinaryOperator::Subtract,
            _ => return Err(IllegalToken(
                self.tokenizer.current_location(), String::from("Cannot use it as a binary operator")
            ))
        };
        let right = self.expression(precedence)?;

        Ok(Expression::Binary(binary_operator, Box::new(left), Box::new(right)))
    }

    fn parse_prefix(&mut self) -> Result<Expression, SimpleDbError> {
        match self.advance()? {
            Token::False => Ok(Expression::Literal(Value::create_boolean(false))),
            Token::True => Ok(Expression::Literal(Value::create_boolean(true))),
            Token::NumberF64(num) => Ok(Expression::Literal(Value::create_f64(num))),
            Token::NumberI64(num) => Ok(Expression::Literal(Value::create_i64(num))),
            Token::String(string) => Ok(Expression::Literal(Value::create_string(string))),
            Token::Identifier(identifier) => Ok(Expression::Identifier(identifier)),
            Token::Minus => Ok(Expression::Unary(UnaryOperator::Minus, Box::new(self.expression(MAX_PRECEDENCE)?))),
            Token::Plus => Ok(Expression::Unary(UnaryOperator::Plus, Box::new(self.expression(MAX_PRECEDENCE)?))),
            Token::OpenParen => {
                let result = self.expression(0)?;
                self.expect_token(Token::CloseParen)?;
                Ok(result)
            },
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Token cannot be parsed")))
        }
    }

    fn limit(&mut self) -> Result<Limit, SimpleDbError> {
        let limit_value = self.number_i64()?;
        Ok(Limit::Some(limit_value as usize))
    }

    fn selection(&mut self) -> Result<Selection, SimpleDbError> {
        if !self.check_last_token(Token::Star) {
            Ok(Selection::Some(self.column_names(&Token::From)?))
        } else {
            self.advance()?; //Consume *
            Ok(Selection::All)
        }
    }

    fn update(&mut self) -> Result<Statement, SimpleDbError> {
        self.advance()?;
        let table_name = self.identifier()?;
        let updated_values = self.updated_values()?;

        let mut expression = None;
        if self.maybe_expect_token(Token::Where)? {
            expression = Some(self.expression(0)?);
        }

        Ok(Statement::Update(UpdateStatement {
            table_name,
            updated_values,
            where_expr: expression
        }))
    }

    fn updated_values(&mut self) -> Result<Vec<(String, Expression)>, SimpleDbError> {
        let mut updated_values = Vec::new();
        while !self.check_last_token(Token::Where) && !self.check_last_token(Token::Semicolon) {
            if updated_values.len() > 0 {
                self.expect_token(Token::Comma)?;
            }

            self.expect_token(Token::Set)?;
            let column_name = self.identifier()?;
            self.expect_token(Token::Equal)?;
            let new_value_expression = self.expression(0)?;

            updated_values.push((column_name, new_value_expression));
        }

        Ok(updated_values)
    }

    fn delete(&mut self) -> Result<Statement, SimpleDbError> {
        self.advance()?;

        self.expect_token(Token::From)?;
        let table_name = self.identifier()?;
        let mut expression = None;
        let mut limit = Limit::None;

        if self.maybe_expect_token(Token::Limit)? {
            limit = self.limit()?;
        }
        if self.maybe_expect_token(Token::Where)? {
            expression = Some(self.expression(0)?);
        }
        if self.maybe_expect_token(Token::Limit)? {
            limit = self.limit()?;
        }

        Ok(Statement::Delete(DeleteStatement{
            table_name,
            where_expr: expression,
            limit,
        }))
    }

    fn commit(&mut self) -> Result<Statement, SimpleDbError> {
        self.advance()?;
        Ok(Statement::Commit)
    }

    fn rollback(&mut self) -> Result<Statement, SimpleDbError> {
        self.advance()?;
        Ok(Statement::Rollback)
    }

    fn start_transaction(&mut self) -> Result<Statement, SimpleDbError> {
        self.advance()?;
        Ok(Statement::StartTransaction)
    }

    fn insert(&mut self) -> Result<Statement, SimpleDbError> {
        self.advance()?;

        self.expect_token(Token::Into)?;
        match self.advance()? {
            Token::Identifier(table_name) => {
                self.expect_token(Token::OpenParen)?;
                let column_names = self.column_names(&Token::CloseParen)?;
                self.expect_token(Token::CloseParen)?;
                self.expect_token(Token::Values)?;
                self.expect_token(Token::OpenParen)?;
                let column_values = self.column_values(&Token::CloseParen)?;
                self.expect_token(Token::CloseParen)?;
                let column_name_values = self.create_insert_statement_values(column_names, column_values)?;

                Ok(Statement::Insert(InsertStatement {
                    values: column_name_values,
                    table_name,
                }))
            },
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Expect table name")))
        }
    }

    fn create_insert_statement_values(
        &self,
        mut column_names: Vec<String>,
        mut column_values_tokens: Vec<Value>
    ) -> Result<Vec<(String, Value)>, SimpleDbError> {
        if column_names.len() != column_values_tokens.len() {
            return Err(SimpleDbError::MalformedQuery(String::from(
                "Insert statements should have the same number of columns and values"
            )));
        }

        let mut insert_values = Vec::new();

        //TODO Check if value can be casted to column type
        while !column_names.is_empty() {
            let column_value = column_values_tokens.pop().unwrap();
            let column_name = column_names.pop().unwrap();

            insert_values.push((column_name, column_value));
        }

        Ok(insert_values)
    }

    fn column_names(&mut self, terminator_token: &Token) -> Result<Vec<String>, SimpleDbError> {
        let mut column_names = Vec::new();
        while !self.check_last_token(terminator_token.clone()) {
            match self.advance()? {
                Token::Identifier(column_name) => column_names.push(column_name),
                _ => return Err(IllegalToken(self.tokenizer.current_location(), String::from("Expected column name")))
            }

            if !self.check_last_token(terminator_token.clone()) {
                self.expect_token(Token::Comma)?;
            }
        }

        Ok(column_names)
    }

    fn column_values(&mut self, terminator_token: &Token) -> Result<Vec<Value>, SimpleDbError> {
        let mut column_values = Vec::new();
        while !self.check_last_token(terminator_token.clone()) {
            let token = self.advance()?;
            let value = token.serialize()
                .map_err(|_| IllegalToken(
                    self.tokenizer.current_location(),
                    String::from("Value cannot be inserted into a row"))
                )?;

            column_values.push(value);

            if !self.check_last_token(terminator_token.clone()) {
                self.expect_token(Token::Comma)?;
            }
        }

        Ok(column_values)
    }

    fn create_some(&mut self) -> Result<Statement, SimpleDbError> {
        self.advance()?;
        match self.advance()? {
            Token::Database => self.create_database(),
            Token::Table => self.create_table(),
            Token::Index => self.create_index(),
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Invalid token after create")))
        }
    }

    fn create_index(&mut self) -> Result<Statement, SimpleDbError> {
        match self.advance()? {
            Token::On => {
                let table_name = self.identifier()?;
                self.expect_token(Token::OpenParen)?;
                let column_name = self.identifier()?;
                self.expect_token(Token::CloseParen)?;
                let is_async = self.maybe_expect_token(Token::Async)?;

                Ok(Statement::CreateIndex(CreateIndexStatement {
                    column_name,
                    table_name,
                    wait: !is_async
                }))
            },
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Expect ON after CREATE INDEX")))
        }
    }

    fn create_table(&mut self) -> Result<Statement, SimpleDbError> {
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

    fn create_database(&mut self) -> Result<Statement, SimpleDbError> {
        match self.advance()? {
            Token::Identifier(database_name) => Ok(Statement::CreateDatabase(database_name)),
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Expect table name")))
        }
    }

    fn create_table_columns(&mut self) -> Result<Vec<(String, Type, bool)>, SimpleDbError> {
        let mut columns = Vec::new();

        while !self.maybe_expect_token(Token::CloseParen)? {
            let table_name = self.identifier()?;
            let column_type = self.column_type()?;
            let is_primary = self.is_primary_key()?;

            columns.push((table_name, column_type, is_primary));

            if !self.check_last_token(Token::CloseParen) {
                self.expect_token(Token::Comma)?;
            }
        }

        Ok(columns)
    }

    fn show(&mut self) -> Result<Statement, SimpleDbError> {
        self.advance()?;

        match self.advance()? {
            Token::Databases => Ok(Statement::ShowDatabases),
            Token::Tables => Ok(Statement::ShowTables),
            Token::Index => self.show_indexes(),
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Expact Databases or tables after show")))
        }
    }

    fn show_indexes(&mut self) -> Result<Statement, SimpleDbError> {
        self.expect_token(Token::From)?;
        let table_name = self.identifier()?;
        Ok(Statement::ShowIndexes(table_name))
    }

    fn describe(&mut self) -> Result<Statement, SimpleDbError> {
        self.advance()?;

        match self.advance()? {
           Token::Identifier(table) => Ok(Statement::Describe(table.clone())),
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Expact table name after describe"))),
        }
    }

    fn is_primary_key(&mut self) -> Result<bool, SimpleDbError> {
        let mut is_primary = false;

        if self.maybe_expect_token(Token::Primary)? {
            self.expect_token(Token::Key)?;
            is_primary = true;
        }

        Ok(is_primary)
    }

    fn column_type(&mut self) -> Result<Type, SimpleDbError> {
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

    fn number_i64(&mut self) -> Result<i64, SimpleDbError> {
        match self.advance()? {
            Token::NumberI64(number) => Ok(number),
            _ => Err(IllegalToken(self.tokenizer.current_location(), String::from("Expected number")))
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
            Err(IllegalToken(
                self.tokenizer.current_location(),
                format!("Expected token {:?} but found {:?}", expected_token, current_token))
            )
        }
    }

    fn check_last_token(&mut self, expected_token: Token) -> bool {
        *self.tokenizer.last_token() == expected_token
    }

    fn get_precedence(&self, token: &Token) -> u8 {
        match token {
            Token::NumberI64(_) | Token::NumberF64(_) | Token::Identifier(_) | Token::String(_) => 0,
            Token::Or => 1,
            Token::And => 2,
            Token::Greater | Token::GreaterEqual | Token::Less | Token::LessEqual | Token::EqualEqual | Token::NotEqual => 3,
            Token::Plus | Token::Minus => 4,
            Token::Slash | Token::Star => 5,
            _ => 0
        }
    }
}

#[cfg(test)]
mod test {
    use shared::{Type, Value};
    use crate::selection::Selection;
    use crate::sql::expression::{BinaryOperator, Expression};
    use crate::sql::parser::parser::Parser;
    use crate::sql::statement::{Limit, Statement};

    #[test]
    fn update_all() {
        let mut parser = Parser::create(String::from("UPDATE personas SET dinero = 0;"));
        let statement = parser.next_statement().unwrap().unwrap();

        assert!(matches!(statement, Statement::Update(_)));
        let update_statement = match statement {
            Statement::Update(u) => u, _ => panic!(),
        };

        assert_eq!(update_statement.table_name, String::from("personas"));
        assert_eq!(update_statement.updated_values.len(), 1);
        assert_eq!(update_statement.updated_values[0].0, "dinero");
        assert_eq!(update_statement.updated_values[0].1, Expression::Literal(Value::create_i64(0)));
        assert_eq!(update_statement.where_expr, None);
    }

    #[test]
    fn update() {
        let mut parser = Parser::create(String::from("UPDATE personas \
            SET dinero = dinero + 10, \
            SET id = 10.2\
            WHERE dinero > 10;"));
        let statement = parser.next_statement().unwrap().unwrap();

        assert!(matches!(statement, Statement::Update(_)));
        let update_statement = match statement {
            Statement::Update(u) => u, _ => panic!(),
        };
        assert_eq!(update_statement.table_name, String::from("personas"));
        assert_eq!(update_statement.updated_values.len(), 2);

        assert_eq!(update_statement.updated_values[0].0, "dinero");
        assert_eq!(update_statement.updated_values[0].1, Expression::Binary(
            BinaryOperator::Add,
            Box::new(Expression::Identifier(String::from("dinero"))),
            Box::new(Expression::Literal(Value::create_i64(10))))
        );
        assert_eq!(update_statement.updated_values[1].0, "id");
        assert_eq!(update_statement.updated_values[1].1, Expression::Literal(Value::create_f64(10.2)));

        assert_eq!(update_statement.where_expr.unwrap(), Expression::Binary(
            BinaryOperator::Greater,
            Box::new(Expression::Identifier(String::from("dinero"))),
            Box::new(Expression::Literal(Value::create_i64(10))),
        ));
    }

    #[test]
    fn select_explain() {
        let mut parser = Parser::create(String::from("EXPLAIN SELECT dinero FROM personas WHERE dinero > 10 LIMIT 10;"));
        let statement = parser.next_statement().unwrap().unwrap();
        assert!(matches!(statement, Statement::Select(_)));

        let select_statement = match statement {
            Statement::Select(s) => s, _ => panic!(),
        };
        assert!(select_statement.explain);
    }

    #[test]
    fn select_with_expression_with_limit() {
        let mut parser = Parser::create(String::from("SELECT dinero FROM personas WHERE dinero > 10 LIMIT 10;"));
        let statement = parser.next_statement().unwrap().unwrap();
        assert!(matches!(statement, Statement::Select(_)));

        let select_statement = match statement {
            Statement::Select(s) => s, _ => panic!(),
        };
        let expected_column_names = vec!["dinero"];
        assert!(matches!(select_statement.selection, Selection::Some(expected_column_names)));
        assert!(matches!(select_statement.limit, Limit::Some(10)));
        assert_eq!(select_statement.table_name, "personas");
        assert_eq!(select_statement.where_expr.unwrap(), Expression::Binary(
            BinaryOperator::Greater,
            Box::new(Expression::Identifier(String::from("dinero"))),
            Box::new(Expression::Literal(Value::create_i64(10))),
        ));
    }

    #[test]
    fn select_without_expression_with_limit() {
        let mut parser = Parser::create(String::from("SELECT nombre, dinero FROM personas LIMIT 10;"));
        let statement = parser.next_statement().unwrap().unwrap();

        assert!(matches!(statement, Statement::Select(_)));
        let select_statement = match statement {
            Statement::Select(s) => s, _ => panic!(),
        };
        let expected_column_names = vec!["nombre", "dinero"];
        assert!(matches!(select_statement.selection, Selection::Some(expected_column_names)));
        assert!(matches!(select_statement.where_expr, None));
        assert!(matches!(select_statement.limit, Limit::Some(10)));
        assert_eq!(select_statement.table_name, "personas");
    }

    #[test]
    fn delete_with_expression() {
        let mut parser = Parser::create(String::from(
            "DELETE FROM personas WHERE id == 1;"
        ));
        let statement = parser.next_statement().unwrap().unwrap();

        assert!(matches!(statement, Statement::Delete(_)));
        let select_statement = match statement {
            Statement::Delete(s) => s, _ => panic!(),
        };
        assert!(matches!(select_statement.limit, Limit::None));
        assert_eq!(select_statement.table_name, "personas");
        assert_eq!(select_statement.where_expr.unwrap(), Expression::Binary(
            BinaryOperator::Equal,
            Box::new(Expression::Identifier(String::from("id"))),
            Box::new(Expression::Literal(Value::create_i64(1))),
        ));
    }

    #[test]
    fn select_with_expression_without_limit() {
        let mut parser = Parser::create(String::from(
            "SELECT * FROM personas WHERE dinero >= 1 + 2 AND nombre == \"Jaime\";"
        ));
        let statement = parser.next_statement().unwrap().unwrap();

        assert!(matches!(statement, Statement::Select(_)));
        let select_statement = match statement {
            Statement::Select(s) => s, _ => panic!(),
        };
        assert!(matches!(select_statement.selection, Selection::All));
        assert!(matches!(select_statement.limit, Limit::None));
        assert_eq!(select_statement.table_name, "personas");

        let expression = select_statement.where_expr;

        assert_eq!(expression.unwrap(), Expression::Binary(
            BinaryOperator::And,
            Box::new(Expression::Binary(
                BinaryOperator::GreaterEqual,
                Box::new(Expression::Identifier(String::from("dinero"))),
                Box::new(Expression::Binary(
                    BinaryOperator::Add,
                    Box::new(Expression::Literal(Value::create_i64(1))),
                    Box::new(Expression::Literal(Value::create_i64(2))),
                )))
            ),
            Box::new(Expression::Binary(
                BinaryOperator::Equal,
                Box::new(Expression::Identifier(String::from("nombre"))),
                Box::new(Expression:: Literal(Value::create_string(String::from("Jaime")))),
            ))
        ));
    }

    #[test]
    fn start_transaction() {
        let mut parser = Parser::create(String::from(
            "START_TRANSACTION;"
        ));
        let statement = parser.next_statement().unwrap().unwrap();
        assert!(matches!(statement, Statement::StartTransaction));
    }

    #[test]
    fn rollback() {
        let mut parser = Parser::create(String::from(
            "ROLLBACK;"
        ));
        let statement = parser.next_statement().unwrap().unwrap();
        assert!(matches!(statement, Statement::Rollback));
    }

    #[test]
    fn commit() {
        let mut parser = Parser::create(String::from(
            "COMMIT;"
        ));
        let statement = parser.next_statement().unwrap().unwrap();
        assert!(matches!(statement, Statement::Commit));
    }

    #[test]
    fn insert() {
        let mut parser = Parser::create(String::from(
            "INSERT INTO personas (id, nombre, dinero) VALUES (1, \"Jaime\", 10.2);"
        ));
        let statement = parser.next_statement().unwrap().unwrap();

        assert!(matches!(statement, Statement::Insert(_)));
        match statement {
            Statement::Insert(insert_statement) => {
                assert_eq!(insert_statement.table_name, String::from("personas"));
                assert_eq!(insert_statement.values.len(), 3);
                assert_eq!(insert_statement.values[2], (String::from("id"), Value::create_i64(1)));
                assert_eq!(insert_statement.values[1], (String::from("nombre"), Value::create_string(String::from("Jaime"))));
                assert_eq!(insert_statement.values[0], (String::from("dinero"), Value::create_f64(10.2)));
            }
            _ => panic!()
        }
    }

    #[test]
    fn describe_table() {
        let mut parser = Parser::create(String::from(
            "DESCRIBE personas;"
        ));
        let statement = parser.next_statement().unwrap().unwrap();

        match statement {
            Statement::Describe(table) => {
                assert_eq!(table, String::from("personas"))
            }
            _ => panic!("")
        }
    }

    #[test]
    fn show_databases() {
        let mut parser = Parser::create(String::from(
            "SHOW DATABASES;"
        ));
        let statement = parser.next_statement().unwrap().unwrap();

        assert!(matches!(statement, Statement::ShowDatabases));
    }

    #[test]
    fn create_table() {
        let mut parser = Parser::create(String::from(
            r#"CREATE TABLE personas (
                id i64 PRIMARY KEY,
                nombre VARCHAR,
                dinero f64
               );"#
        ));
        let statement = parser.next_statement().unwrap().unwrap();

        assert!(matches!(statement, Statement::CreateTable(_)));
        match statement {
            Statement::CreateTable(createStatement) => {
                assert_eq!(createStatement.table_name, String::from("personas"));
                assert_eq!(createStatement.columns.len(), 3);
                assert_eq!(createStatement.columns[0], (String::from("id"), Type::I64, true));
                assert_eq!(createStatement.columns[1], (String::from("nombre"), Type::String, false));
                assert_eq!(createStatement.columns[2], (String::from("dinero"), Type::F64, false));
            },
            _ => panic!()
        }
    }

    #[test]
    fn show_indexes() {
        let mut parser = Parser::create(String::from(
            "SHOW INDEX FROM personas;"
        ));
        let statement = parser.next_statement().unwrap().unwrap();

        match statement {
            Statement::ShowIndexes(table_name) => assert_eq!(table_name, String::from("personas")),
            _ => panic!(""),
        }
    }

    #[test]
    fn create_index() {
        let mut parser = Parser::create(String::from(
            "CREATE INDEX ON personas (dinero) ASYNC;"
        ));
        let statement = parser.next_statement().unwrap().unwrap();

        match statement {
            Statement::CreateIndex(statement) => {
                assert_eq!(statement.table_name, String::from("personas"));
                assert_eq!(statement.column_name, String::from("dinero"));
                assert_eq!(statement.wait, false);
            }
            _ => panic!("")
        }
    }
}