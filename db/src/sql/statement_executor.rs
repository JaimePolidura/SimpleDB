use crate::sql::statement::{CreateTableStatement, InsertStatement, Statement};
use crate::sql::statement_result::StatementResult;
use crate::{ColumnType, Database, Table};
use shared::{utils, SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use bytes::Bytes;
use storage::transactions::transaction::Transaction;
use crate::sql::token::Token::Values;

pub struct StatementExecutor {
    options: Arc<SimpleDbOptions>
}

impl StatementExecutor {
    pub fn create(options: &Arc<SimpleDbOptions>) -> StatementExecutor {
        StatementExecutor {
            options: options.clone()
        }
    }

    pub fn execute(
        &self,
        transaction: &Option<Transaction>,
        database: Arc<Database>,
        statement: Statement,
    ) -> Result<StatementResult, SimpleDbError> {
        match statement {
            Statement::Select(select_statement) => todo!(),
            Statement::Update(update_statement) => todo!(),
            Statement::Delete(delete_statement) => todo!(),
            Statement::Insert(insert_statement) => self.insert(database, transaction, insert_statement),
            Statement::CreateTable(create_table_statement) => self.create_table(database, create_table_statement),
            Statement::StartTransaction => self.start_transaction(database),
            Statement::Rollback => self.rollback_transaction(database, transaction),
            Statement::Commit => self.commit_transaction(database, transaction),
        }
    }

    fn insert(
        &self,
        database: Arc<Database>,
        transaction: &Option<Transaction>,
        mut insert_statement: InsertStatement,
    ) -> Result<StatementResult, SimpleDbError> {
        let table = database.get_table(insert_statement.table_name.as_str())?;
        let transaction = transaction.as_ref().unwrap();
        table.validate_column_values(&mut insert_statement.values)?;
        let mut inserted_values = self.format_column_values(&table, &insert_statement.values);
        table.insert(transaction, &mut inserted_values)?;
        Ok(StatementResult::Ok(1))
    }

    fn create_table(
        &self,
        database: Arc<Database>,
        create_table_statement: CreateTableStatement,
    ) -> Result<StatementResult, SimpleDbError> {
        database.validate_create_table(create_table_statement.table_name.as_str(), &create_table_statement.columns)?;
        database.create_table(create_table_statement.table_name.as_str(), create_table_statement.columns)?;
        Ok(StatementResult::Ok(0))
    }

    fn start_transaction(
        &self,
        database: Arc<Database>
    ) -> Result<StatementResult, SimpleDbError> {
        let transaction = database.start_transaction();
        Ok(StatementResult::TransactionStarted(transaction))
    }

    fn rollback_transaction(
        &self,
        database: Arc<Database>,
        transaction: &Option<Transaction>
    ) -> Result<StatementResult, SimpleDbError> {
        database.rollback_transaction(transaction.as_ref().unwrap())?;
        Ok(StatementResult::Ok(0))
    }

    fn commit_transaction(
        &self,
        database: Arc<Database>,
        transaction: &Option<Transaction>
    ) -> Result<StatementResult, SimpleDbError> {
        database.commit_transaction(transaction.as_ref().unwrap())?;
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
                ColumnType::BOOLEAN => unformatted_column_value.clone(),
                ColumnType::VARCHAR => unformatted_column_value.clone(),
                ColumnType::DATE => unformatted_column_value.clone(),
                ColumnType::BLOB => unformatted_column_value.clone()
            };

            formatted_values.push((column_name.clone(), formatted_column_value));
        }

        formatted_values
    }
}