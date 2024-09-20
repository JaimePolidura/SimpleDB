use crate::database::databases::Databases;
use crate::sql::parser::Parser;
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use crate::sql::statement_executor::StatementExecutor;
use crate::sql::statement_result::StatementResult;

pub struct SimpleDb {
    statement_executor: StatementExecutor,
    databases: Databases,

    options: Arc<SimpleDbOptions>
}

pub struct SimpleDbTransaction {
    transaction: Option<Transaction>,
}

impl SimpleDb {
    pub fn create(
        options: SimpleDbOptions,
    ) -> Result<SimpleDb, SimpleDbError> {
        let options = Arc::new(options);

        Ok(SimpleDb {
            databases: Databases::create(options.clone())?,
            statement_executor: StatementExecutor::create(&options),
            options,
        })
    }

    pub fn execute(
        &self,
        transaction: Option<Transaction>,
        database: &str,
        query: String,
    ) -> Result<Vec<StatementResult>, SimpleDbError> {
        let database = self.databases.get_database(database)
            .ok_or(SimpleDbError::DatabaseNotFound(query.clone()))?;
        let mut parser = Parser::create(query);
        let mut results = Vec::new();

        while let Some(statement) = parser.next_statement()? {
            let has_ownership_over_transaction = statement.has_ownership_over_transaction();
            let result = self.statement_executor.execute(
                &transaction,
                database.clone(),
                statement
            )?;
            results.push(result);

            //No more statements will be run after a commit or a rollback
            if has_ownership_over_transaction {
                break
            }
        }

        Ok(results)
    }
}