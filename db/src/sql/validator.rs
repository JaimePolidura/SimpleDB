use crate::database::databases::Databases;
use crate::simple_db::Context;
use crate::sql::expression::Expression;
use crate::sql::statement::{CreateTableStatement, DeleteStatement, InsertStatement, SelectStatement, Statement, UpdateStatement};
use crate::table::table::Table;
use crate::value::Type;
use crate::CreateIndexStatement;
use shared::SimpleDbError::UnknownColumn;
use shared::SimpleDbError;
use std::sync::Arc;

pub struct StatementValidator {
    databases: Arc<Databases>,
}

impl StatementValidator {
    pub fn create(
        databases: &Arc<Databases>,
    ) -> StatementValidator {
        StatementValidator {
            databases: databases.clone(),
        }
    }

    pub fn validate(
        &self,
        context: &Context,
        statement: &Statement,
    ) -> Result<(), SimpleDbError> {
        self.validate_context(context, statement)?;

        match statement {
            Statement::CreateIndex(statement) => self.validate_create_secondary_index(statement, context.database()),
            Statement::CreateTable(statement) => self.validate_create_table(context.database(), statement),
            Statement::Select(statement) => self.validate_select(context.database(), statement),
            Statement::Update(statement) => self.validate_update(context.database(), statement),
            Statement::Delete(statement) => self.validate_delete(context.database(), statement),
            Statement::Insert(statement) => self.validate_insert(context.database(), statement),
            Statement::CreateDatabase(database_name) => self.validate_create_database(database_name),
            Statement::ShowIndexes(table_name) => self.validate_show_indexes(context.database(), table_name),
            Statement::Describe(table) => self.validate_describe(context, table),
            Statement::StartTransaction |
            Statement::ShowDatabases |
            Statement::ShowTables |
            Statement::Rollback |
            Statement::Commit => Ok(()),
        }
    }

    fn validate_create_secondary_index(
        &self,
        statement: &CreateIndexStatement,
        database_name: &str,
    ) -> Result<(), SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table_or_err(&statement.table_name)?;
        table.validate_create_index(&statement.column_name)
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
        let table = database.get_table_or_err(&statement.table_name)?;
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
        let table = database.get_table_or_err(&statement.table_name)?;
        self.validate_where_expression(&statement.where_expr, &table)?;

        for (updated_column_name, updated_column_value_expr) in &statement.updated_values {
            let column_data = table.get_column_desc(updated_column_name)
                .ok_or(SimpleDbError::ColumnNotFound(table.storage_keyspace_id, updated_column_name.clone()))?;
            let expression_type_result = self.validate_expression(updated_column_value_expr, &table)?;

            if !expression_type_result.can_be_casted(&column_data.column_type) {
                return Err(SimpleDbError::MalformedQuery(String::from("SET expression should produce a column value")))
            }
        }

        Ok(())
    }

    fn validate_show_indexes(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> Result<(), SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let _ = database.get_table_or_err(table_name)?;
        Ok(())
    }

    fn validate_insert(
        &self,
        database_name: &String,
        statement: &InsertStatement
    ) -> Result<(), SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table_or_err(statement.table_name.as_str())?;
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
        let table = database.get_table_or_err(&statement.table_name)?;
        self.validate_where_expression(&statement.where_expr, &table)?;
        Ok(())
    }

    fn validate_where_expression(
        &self,
        expression: &Option<Expression>,
        table: &Arc<Table>
    ) -> Result<(), SimpleDbError> {
        match expression {
            Some(expression) => {
                let type_produced = self.validate_expression(expression, &table)?;
                if !matches!(type_produced, Type::Boolean) {
                    Err(SimpleDbError::MalformedQuery(String::from("Expression should produce a boolean")))
                } else {
                    Ok(())
                }
            },
            None => Ok(())
        }
    }

    fn validate_describe(&self, context: &Context, table_name: &str) -> Result<(), SimpleDbError> {
        let database = self.databases.get_database_or_err(context.database())?;
        database.get_table_or_err(table_name)?;
        Ok(())
    }

    fn validate_expression(
        &self,
        expression: &Expression,
        table: &Arc<Table>
    ) -> Result<Type, SimpleDbError> {
        match expression {
            Expression::Binary(operator, left, right) => {
                let type_left = self.validate_expression(left, table)?;
                let type_right = self.validate_expression(right, table)?;

                if operator.is_logical() &&
                    matches!(type_left, Type::Boolean) &&
                    matches!(type_right, Type::Boolean) {
                    Ok(Type::Boolean)
                } else if operator.is_arithmetic() &&
                    type_left.is_number() &&
                    type_right.is_number() {

                    if type_left.is_fp_number() || type_right.is_fp_number() {
                        Ok(Type::F64)
                    } else if type_left.is_signed_integer_number() || type_right.is_signed_integer_number() {
                        Ok(Type::I64)
                    } else {
                        Ok(Type::U64)
                    }
                } else if operator.is_comparation() && type_left.is_comparable(&type_right) {
                    Ok(Type::Boolean)
                } else {
                    Err(SimpleDbError::MalformedQuery(String::from("Expression produces wrong type")))
                }
            },
            Expression::Unary(_, expr) => {
                let produced_type = self.validate_expression(expr, table)?;
                if !produced_type.is_number() {
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
            Expression::Literal(value) => Ok(value.to_type()),
        }
    }

    fn validate_context(&self, context: &Context, statement: &Statement) -> Result<(), SimpleDbError> {
        if statement.requires_transaction() && !context.has_transaction() {
            return Err(SimpleDbError::InvalidContext("A Transaction should be supplied"));
        }
        if statement.requires_database() && !context.has_database() {
            return Err(SimpleDbError::InvalidContext("A Database should be supplied"));
        }

        Ok(())
    }
}
