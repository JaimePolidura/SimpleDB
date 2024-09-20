use crate::sql::statement::Statement;
use crate::sql::statement_result::StatementResult;
use crate::Database;
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;

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
            Statement::Insert(insert_statement) => todo!(),
            Statement::CreateTable(create_table_statement) => todo!(),
            Statement::StartTransaction => self.start_transaction(database),
            Statement::Rollback => self.rollback_transaction(database, transaction),
            Statement::Commit => self.commit_transaction(database, transaction),
        }
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
}