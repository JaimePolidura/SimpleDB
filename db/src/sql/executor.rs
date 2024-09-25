use crate::database::databases::Databases;
use crate::simple_db::{Context, StatementResult};
use crate::sql::expression::Expression;
use crate::sql::expression_evaluator::{evaluate_constant_expressions, evaluate_expression};
use crate::sql::plan::planner::Planner;
use crate::sql::query_iterator::QueryIterator;
use crate::sql::statement::{CreateTableStatement, DeleteStatement, InsertStatement, SelectStatement, Statement, UpdateStatement};
use crate::sql::validator::StatementValidator;
use crate::table::column_type::ColumnType;
use crate::table::table::Table;
use bytes::Bytes;
use shared::{utils, SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;

pub struct StatementExecutor {
    options: Arc<SimpleDbOptions>,
    databases: Arc<Databases>,

    validator: StatementValidator,
    planner: Planner
}

impl StatementExecutor {
    pub fn create(options: &Arc<SimpleDbOptions>, databases: &Arc<Databases>) -> StatementExecutor {
        StatementExecutor {
            validator: StatementValidator::create(databases, options),
            planner: Planner::create(options.clone()),
            options: options.clone(),
            databases: databases.clone()
        }
    }

    pub fn execute(
        &self,
        context: &Context,
        statement: Statement,
    ) -> Result<StatementResult, SimpleDbError> {
        self.validator.validate(context, &statement)?;
        let statement = self.evaluate_constant_expressions(statement)?;

        match statement {
            Statement::Select(select_statement) => self.select(context.database(), context.transaction(), select_statement),
            Statement::Update(update_statement) => self.update(context.database(), context.transaction(), update_statement),
            Statement::Delete(delete_statement) => self.delete(context.database(), context.transaction(), delete_statement),
            Statement::Insert(insert_statement) => self.insert(context.database(), context.transaction(), insert_statement),
            Statement::CreateTable(create_table_statement) => self.create_table(context.database(), create_table_statement),
            Statement::StartTransaction => self.start_transaction(context.database()),
            Statement::Rollback => self.rollback_transaction(context.database(), context.transaction()),
            Statement::Commit => self.commit_transaction(context.database(), context.transaction()),
            Statement::CreateDatabase(database_name) => self.create_databse(database_name)
        }
    }

    fn select(
        &self,
        database_name: &String,
        transaction: &Transaction,
        select_statement: SelectStatement,
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table(&select_statement.table_name)?;
        let select_plan = self.planner.plan_select(&table, select_statement, transaction)?;

        Ok(StatementResult::Data(QueryIterator::create(select_plan)))
    }

    fn update(
        &self,
        database_name: &String,
        transaction: &Transaction,
        update_statement: UpdateStatement,
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table(&update_statement.table_name)?;
        let mut update_plan = self.planner.plan_update(&table, &update_statement, transaction)?;
        let mut updated_rows = 0;

        while let Some(row_to_update) = update_plan.next()? {
            let id = row_to_update.get_primary_column_value().clone();
            let mut new_values = Vec::new();

            for (updated_column_name, new_value_expr) in &update_statement.updated_values {
                let new_value_bytes = match evaluate_expression(&row_to_update, new_value_expr)? {
                    Expression::Null => continue,
                    other => other.serialize(),
                };

                new_values.push((updated_column_name.clone(), new_value_bytes));
            }

            table.update(transaction, id, &new_values)?;
            updated_rows += 1;
        }

        Ok(StatementResult::Ok(updated_rows))
    }

    fn delete(
        &self,
        database_name: &String,
        transaction: &Transaction,
        delete_statement: DeleteStatement
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table(delete_statement.table_name.as_str())?;
        let mut delete_plan = self.planner.plan_delete(&table, delete_statement, transaction)?;
        let mut deleted_rows = 0;

        while let Some(row_to_delete) = delete_plan.next()? {
            let id = row_to_delete.get_primary_column_value();
            table.delete(transaction, id.clone())?;
            deleted_rows += 1;
        }

        Ok(StatementResult::Ok(deleted_rows))
    }

    fn insert(
        &self,
        database_name: &String,
        transaction: &Transaction,
        mut insert_statement: InsertStatement,
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table(insert_statement.table_name.as_str())?;
        let mut inserted_values = self.format_column_values(&table, &insert_statement.values);
        table.insert(transaction, &mut inserted_values)?;
        Ok(StatementResult::Ok(1))
    }

    fn create_table(
        &self,
        database_name: &String,
        create_table_statement: CreateTableStatement,
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        database.create_table(create_table_statement.table_name.as_str(), create_table_statement.columns)?;
        Ok(StatementResult::Ok(0))
    }

    fn start_transaction(
        &self,
        database_name: &String
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let transaction = database.start_transaction();
        Ok(StatementResult::TransactionStarted(transaction))
    }

    fn rollback_transaction(
        &self,
        database_name: &String,
        transaction: &Transaction
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        database.rollback_transaction(transaction)?;
        Ok(StatementResult::Ok(0))
    }

    fn commit_transaction(
        &self,
        database_name: &String,
        transaction: &Transaction
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        database.commit_transaction(transaction)?;
        Ok(StatementResult::Ok(0))
    }

    fn create_databse(
        &self,
        database_name: String
    ) -> Result<StatementResult, SimpleDbError> {
        self.databases.create_database(database_name.as_str())?;
        Ok(StatementResult::Ok(0))
    }

    fn format_column_values(
        &self,
        table: &Arc<Table>,
        values: &Vec<(String, Bytes)>
    ) -> Vec<(String, Bytes)>{
        let mut formatted_values = Vec::new();
        let columns = table.get_columns();

        for (column_name, unformatted_column_value) in values {
            let column_desc = columns.get(column_name).unwrap();
            let formatted_column_value = match column_desc.column_type {
                ColumnType::I8 => Bytes::from(utils::bytes_to_i8(unformatted_column_value).to_le_bytes().to_vec()),
                ColumnType::U8 => Bytes::from(utils::bytes_to_u8(unformatted_column_value).to_le_bytes().to_vec()),
                ColumnType::I16 => Bytes::from(utils::bytes_to_i16_le(unformatted_column_value).to_le_bytes().to_vec()),
                ColumnType::U16 => Bytes::from(utils::bytes_to_u16_le(unformatted_column_value).to_le_bytes().to_vec()),
                ColumnType::U32 => Bytes::from(utils::bytes_to_u32_le(unformatted_column_value).to_le_bytes().to_vec()),
                ColumnType::I32 => Bytes::from(utils::bytes_to_i32_le(unformatted_column_value).to_le_bytes().to_vec()),
                ColumnType::U64 => Bytes::from(utils::bytes_to_u64_le(unformatted_column_value).to_le_bytes().to_vec()),
                ColumnType::I64 => Bytes::from(utils::bytes_to_i64_le(unformatted_column_value).to_le_bytes().to_vec()),
                ColumnType::F32 => Bytes::from(utils::bytes_to_f32_le(unformatted_column_value).to_le_bytes().to_vec()),
                ColumnType::F64 => Bytes::from(utils::bytes_to_f64_le(unformatted_column_value).to_le_bytes().to_vec()),
                ColumnType::Boolean => unformatted_column_value.clone(),
                ColumnType::Varchar => unformatted_column_value.clone(),
                ColumnType::Date => unformatted_column_value.clone(),
                ColumnType::Blob => unformatted_column_value.clone(),
                ColumnType::Null => unformatted_column_value.clone()
            };

            formatted_values.push((column_name.clone(), formatted_column_value));
        }

        formatted_values
    }

    fn evaluate_constant_expressions(&self, mut statement: Statement) -> Result<Statement, SimpleDbError> {
        match statement {
            Statement::Select(mut select) => {
                select.where_expr = evaluate_constant_expressions(select.where_expr)?;
                Ok(Statement::Select(select))
            }
            Statement::Update(mut update) => {
                update.where_expr = evaluate_constant_expressions(update.where_expr)?;
                Ok(Statement::Update(update))
            }
            Statement::Delete(mut delete) => {
                delete.where_expr = evaluate_constant_expressions(delete.where_expr)?;
                Ok(Statement::Delete(delete))
            },
            _ => Ok(statement)
        }
    }
}