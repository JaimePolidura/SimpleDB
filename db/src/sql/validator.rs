use crate::database::databases::Databases;
use crate::sql::expression::Expression;
use crate::sql::statement::{CreateTableStatement, DeleteStatement, InsertStatement, SelectStatement, Statement, UpdateStatement};
use crate::table::column_type::ColumnType;
use crate::table::table::Table;
use shared::SimpleDbError::UnknownColumn;
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use crate::simple_db::Context;

pub struct StatementValidator {
    databases: Arc<Databases>,

    options: Arc<SimpleDbOptions>,
}

impl StatementValidator {
    pub fn create(
        databases: &Arc<Databases>,
        options: &Arc<SimpleDbOptions>
    ) -> StatementValidator {
        StatementValidator {
            databases: databases.clone(),
            options: options.clone(),
        }
    }

    pub fn validate(
        &self,
        context: &Context,
        statement: &Statement,
    ) -> Result<(), SimpleDbError> {
        match statement {
            Statement::Select(statement) => self.validate_select(context.database(), statement),
            Statement::Update(statement) => self.validate_update(context.database(), statement),
            Statement::Delete(statement) => self.validate_delete(context.database(), statement),
            Statement::Insert(statement) => self.validate_insert(context.database(), statement),
            Statement::CreateTable(statement) => self.validate_create_table(context.database(), statement),
            Statement::CreateDatabase(database_name) => self.validate_create_database(database_name),
            Statement::Rollback |
            Statement::Commit |
            Statement::StartTransaction => {
                Ok(())
            }
        }
    }

    fn validate_create_database(
        &self,
        database_name: &String
    ) -> Result<(), SimpleDbError> {
        match self.databases.get_database(database_name) {
            Some(_) => Err(SimpleDbError::DatabaseAlreadyExists(database_name.to_string())),
            None => Ok(())
        }
    }

    fn validate_select(
        &self,
        database_name: &String,
        statement: &SelectStatement
    ) -> Result<(), SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table(&statement.table_name)?;
        self.validate_where_expression(&statement.where_expr, &table)?;
        table.validate_selection(&statement.selection)?;
        Ok(())
    }

    fn validate_update(
        &self,
        database_name: &String,
        statement: &UpdateStatement
    ) -> Result<(), SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table(&statement.table_name)?;
        self.validate_where_expression(&statement.where_expr, &table)?;

        for (updated_column_name, updated_column_value_expr) in &statement.updated_values {
            let column_data = table.get_column_desc(updated_column_name)
                .ok_or(SimpleDbError::ColumnNotFound(table.storage_keyspace_id, updated_column_name.clone()))?;
            let expression_type_result = self.validate_expression(updated_column_value_expr, &table)?;

            if !expression_type_result.can_be_casted(column_data.column_type) {
                return Err(SimpleDbError::MalformedQuery(String::from("SET expression should produce a column value")))
            }
        }

        Ok(())
    }

    fn validate_insert(
        &self,
        database_namae: &String,
        statement: &InsertStatement
    ) -> Result<(), SimpleDbError> {
        let database = self.databases.get_database_or_err(database_namae)?;
        let table = database.get_table(statement.table_name.as_str())?;
        table.validate_column_values(&statement.values)
    }

    fn validate_create_table(
        &self,
        database_name: &String,
        statement: &CreateTableStatement
    ) -> Result<(), SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        database.validate_create_table(&statement)
    }

    fn validate_delete(
        &self,
        database_name: &String,
        statement: &DeleteStatement,
    ) -> Result<(), SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
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
        if !matches!(type_produced, ColumnType::Boolean) {
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
                    matches!(type_left, ColumnType::Boolean) &&
                    matches!(type_right, ColumnType::Boolean) {
                    Ok(ColumnType::Boolean)
                } else if operator.is_arithmetic() &&
                    type_left.is_numeric() &&
                    type_right.is_numeric() {
                    Ok(type_left.get_arithmetic_produced_type(type_right))
                } else if operator.is_comparation() && type_left.is_comparable(type_right) {
                    Ok(ColumnType::Boolean)
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
                table.get_column_desc(table_name)
                    .ok_or(UnknownColumn(table_name.clone()))
                    .map(|it| it.column_type)
            },
            Expression::String(_) => Ok(ColumnType::Varchar),
            Expression::Boolean(_) => Ok(ColumnType::Boolean),
            Expression::NumberF64(_) => Ok(ColumnType::F64),
            Expression::NumberI64(_) => Ok(ColumnType::I64),
            Expression::Null => Ok(ColumnType::Null),
        }
    }
}
