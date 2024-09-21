use std::sync::Arc;
use shared::{SimpleDbError, SimpleDbOptions};
use shared::SimpleDbError::UnknownColumn;
use crate::{ColumnType, Database, Table};
use crate::sql::expression::{BinaryOperator, Expression};
use crate::sql::statement::{CreateTableStatement, DeleteStatement, InsertStatement, Statement};

pub struct StatementValidator {
    options: Arc<SimpleDbOptions>
}

impl StatementValidator {
    pub fn create(
        options: &Arc<SimpleDbOptions>
    ) -> StatementValidator {
        StatementValidator { options: options.clone() }
    }

    pub fn validate(
        &self,
        database: &Arc<Database>,
        statement: &Statement,
    ) -> Result<(), SimpleDbError> {
        match statement {
            Statement::Select(_) => {}
            Statement::Update(_) => {}
            Statement::Delete(statement) => self.validate_delete(database, statement),
            Statement::Insert(statement) => self.validate_insert(database, statement),
            Statement::CreateTable(statement) => self.validate_create_table(database, statement),
            Statement::Rollback |
            Statement::Commit |
            Statement::StartTransaction => {
                Ok(())
            }
        }
    }

    fn validate_insert(
        &self,
        database: &Arc<Database>,
        statement: &InsertStatement
    ) -> Result<(), SimpleDbError> {
        let table = database.get_table(statement.table_name.as_str())?;
        table.validate_column_values(&statement.values)
    }

    fn validate_create_table(
        &self,
        database: &Arc<Database>,
        statement: &CreateTableStatement
    ) -> Result<(), SimpleDbError> {
        database.validate_create_table(&statement.table_name, &statement.columns)
    }

    fn validate_delete(
        &self,
        database: &Arc<Database>,
        statement: &DeleteStatement,
    ) -> Result<(), SimpleDbError> {
        let table = database.get_table(&statement.table_name)?;
        self.validate_where_expression(&statement.where_expr, &table)?;
        Ok(())
    }

    fn validate_where_expression(
        &self,
        expression: &Expression,
        table: &Arc<Table>
    ) -> Result<(), SimpleDbError> {
        let type_produced = self.validate_expression(expression, &table)?;
        if !matches!(type_produced, ColumnType::BOOLEAN) {
            Err(SimpleDbError::MalformedQuery(String::from("Expression should produce a boolean")))
        } else {
            Ok(())
        }
    }

    fn validate_expression(
        &self,
        expression: &Expression,
        table: &Arc<Table>
    ) -> Result<ColumnType, SimpleDbError> {
        match expression {
            Expression::None => panic!(""),
            Expression::Binary(operator, left, right) => {
                let type_left = self.validate_expression(left, table)?;
                let type_right = self.validate_expression(right, table)?;

                if operator.is_logical() &&
                    matches!(type_left, ColumnType::BOOLEAN) &&
                    matches!(type_right, ColumnType::BOOLEAN) {
                    Ok(ColumnType::BOOLEAN)
                } else if operator.is_arithmetic() &&
                    type_left.is_numeric() &&
                    type_right.is_numeric() {
                    Ok(type_left.get_arithmetic_produced_type(type_right))
                } else if operator.is_comparation() && type_left.is_comparable(type_right) {
                    Ok(ColumnType::BOOLEAN)
                } else {
                    Err(SimpleDbError::MalformedQuery(String::from("Expression produces wrong type")))
                }
            },
            Expression::Unary(_, expr) => {
                let produced_type = self.validate_expression(expr, table)?;
                if !produced_type.is_numeric() {
                    Err(SimpleDbError::MalformedQuery(String::from("Expression should produce a number")))
                } else {
                    Ok(produced_type)
                }
            }
            Expression::Identifier(table_name) => {
                table.get_column_data(table_name)
                    .ok_or(UnknownColumn(table_name.clone()))
                    .map(|it| it.column_type)
            },
            Expression::String(_) => Ok(ColumnType::VARCHAR),
            Expression::Boolean(_) => Ok(ColumnType::BOOLEAN),
            Expression::NumberF64(_) => Ok(ColumnType::F64),
            Expression::NumberI64(_) => Ok(ColumnType::I64)
        }
    }
}
