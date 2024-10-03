use crate::database::databases::Databases;
use crate::selection::Selection;
use crate::simple_db::{Context, StatementResult};
use crate::sql::expression::Expression;
use crate::sql::expression_evaluator::{evaluate_constant_expressions, evaluate_expression};
use crate::sql::plan::planner::Planner;
use crate::sql::query_iterator::QueryIterator;
use crate::sql::statement::{CreateTableStatement, DeleteStatement, InsertStatement, SelectStatement, Statement, UpdateStatement};
use crate::sql::validator::StatementValidator;
use crate::table::table::Table;
use crate::value::Value;
use crate::{ColumnDescriptor, CreateIndexStatement};
use bytes::Bytes;
use shared::SimpleDbError::MalformedQuery;
use shared::{SimpleDbError, SimpleDbOptions};
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
            Statement::CreateIndex(statement) => self.create_secondary_index(context.database(), statement),
            Statement::Rollback => self.rollback_transaction(context.database(), context.transaction()),
            Statement::Commit => self.commit_transaction(context.database(), context.transaction()),
            Statement::CreateDatabase(database_name) => self.create_database(database_name),
            Statement::Describe(table_name) => self.describe_table(&table_name, context),
            Statement::StartTransaction => self.start_transaction(context.database()),
            Statement::ShowTables => self.show_tables(&context),
            Statement::ShowDatabases => self.show_databases(),
        }
    }

    fn select(
        &self,
        database_name: &String,
        transaction: &Transaction,
        select_statement: SelectStatement,
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table_or_err(&select_statement.table_name)?;
        let columns_desc = self.get_column_desc_by_selection(&select_statement.selection, &table);
        let select_plan = self.planner.plan_select(&table, select_statement, transaction)?;

        Ok(StatementResult::Data(QueryIterator::create(select_plan, columns_desc)))
    }

    fn update(
        &self,
        database_name: &String,
        transaction: &Transaction,
        update_statement: UpdateStatement,
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table_or_err(&update_statement.table_name)?;
        let mut update_plan = self.planner.plan_update(&table, &update_statement, transaction)?;
        let mut updated_rows = 0;

        while let Some(row_to_update) = update_plan.next()? {
            let id = row_to_update.get_primary_column_value().clone();
            let mut new_values = Vec::new();

            for (updated_column_name, new_value_expr) in &update_statement.updated_values {
                let new_value_bytes = match evaluate_expression(&row_to_update, new_value_expr)? {
                    Expression::Literal(updated_value) => {
                        if !updated_value.is_null() {
                            updated_value.serialize()
                        } else {
                            continue
                        }
                    },
                    _ => return Err(MalformedQuery(String::from("Update values should produce a literal value")))
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
        let table = database.get_table_or_err(delete_statement.table_name.as_str())?;
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
        let table = database.get_table_or_err(insert_statement.table_name.as_str())?;
        let mut inserted_values = self.serialize_column_values(&insert_statement.values);
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

    fn create_secondary_index(
        &self,
        database_name: &String,
        statement: CreateIndexStatement,
    ) -> Result<StatementResult, SimpleDbError> {
        let database = self.databases.get_database_or_err(database_name)?;
        let table = database.get_table_or_err(&statement.table_name)?;

        let n_affected_rows = table.create_secondary_index(
            &statement.column_name, statement.wait
        )?;

        Ok(StatementResult::Ok(n_affected_rows))
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

    fn create_database(
        &self,
        database_name: String
    ) -> Result<StatementResult, SimpleDbError> {
        self.databases.create_database(database_name.as_str())?;
        Ok(StatementResult::Ok(0))
    }

    fn show_databases(&self) -> Result<StatementResult, SimpleDbError> {
        let databases_name = self.databases.get_databases()
            .iter()
            .map(|database| database.name().clone())
            .collect();

        Ok(StatementResult::Databases(databases_name))
    }

    fn show_tables(&self, context: &Context) -> Result<StatementResult, SimpleDbError> {
        let databsae = self.databases.get_database_or_err(context.database())?;
        let table_names = databsae.get_tables().iter()
            .map(|table| table.name().clone())
            .collect();
        Ok(StatementResult::Tables(table_names))
    }

    fn describe_table(&self, table_name: &str, context: &Context) -> Result<StatementResult, SimpleDbError> {
        let databases = self.databases.get_database_or_err(context.database())?;
        let table = databases.get_table_or_err(table_name)?;
        let column_descriptors = table.get_columns().values()
            .map(|entry| entry.clone())
            .collect();
        Ok(StatementResult::Describe(column_descriptors))
    }

    fn serialize_column_values(
        &self,
        values: &Vec<(String, Value)>
    ) -> Vec<(String, Bytes)>{
        let mut formatted_values = Vec::new();
        for (column_name, column_value) in values {
            formatted_values.push((column_name.clone(), column_value.serialize()));
        }

        formatted_values
    }

    fn evaluate_constant_expressions(&self, mut statement: Statement) -> Result<Statement, SimpleDbError> {
        match statement {
            Statement::Select(mut select) => {
                if let Some(where_expr) = select.where_expr {
                    select.where_expr = Some(evaluate_constant_expressions(where_expr)?);
                }

                Ok(Statement::Select(select))
            }
            Statement::Update(mut update) => {
                if let Some(where_expr) = update.where_expr {
                    update.where_expr = Some(evaluate_constant_expressions(where_expr)?);
                }

                Ok(Statement::Update(update))
            }
            Statement::Delete(mut delete) => {
                if let Some(where_expr) = delete.where_expr {
                    delete.where_expr = Some(evaluate_constant_expressions(where_expr)?);
                }

                Ok(Statement::Delete(delete))
            },
            _ => Ok(statement)
        }
    }

    fn get_column_desc_by_selection(
        &self,
        selection: &Selection,
        table: &Arc<Table>,
    ) -> Vec<ColumnDescriptor> {
        match selection {
            Selection::Some(columns) => {
                let mut columns_desc = Vec::new();
                for column_name in columns {
                    columns_desc.push(table.get_column_desc(column_name).unwrap().clone());
                }
                columns_desc
            },
            Selection::All => {
                table.get_columns().values()
                    .map(|it| it.clone())
                    .collect()
            }
        }
    }
}