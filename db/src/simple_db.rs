use crate::database::databases::Databases;
use crate::sql::executor::StatementExecutor;
use crate::sql::parser::parser::Parser;
use crate::sql::query_iterator::QueryIterator;
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use crate::table::table_descriptor::ColumnDescriptor;

pub struct SimpleDb {
    statement_executor: StatementExecutor,

    databases: Arc<Databases>,

    options: Arc<SimpleDbOptions>
}

pub enum StatementResult {
    TransactionStarted(Transaction),
    Data(QueryIterator),
    Ok(usize), //usize number of rows affected
    Databases(Vec<String>),
    Tables(Vec<String>),
    Describe(Vec<ColumnDescriptor>)
}

pub fn create(
    options: SimpleDbOptions,
) -> Result<SimpleDb, SimpleDbError> {
    let options = Arc::new(options);
    let databases = Arc::new(Databases::create(options.clone())?);

    Ok(SimpleDb {
        statement_executor: StatementExecutor::create(&options, &databases),
        databases,
        options,
    })
}

pub struct Context {
    transaction: Option<Transaction>,
    database: Option<String>,
}

impl SimpleDb {
    pub fn execute(
        &self,
        context: &Context,
        query: &str,
    ) -> Result<Vec<StatementResult>, SimpleDbError> {
        let mut parser = Parser::create(query.to_string());
        let mut results = Vec::new();

        while let Some(statement) = parser.next_statement()? {
            let terminates_transaction = statement.terminates_transaction();

            let result = self.statement_executor.execute(
                &context,
                statement
            )?;

            results.push(result);

            //No more statements will be run after a commit or a rollback
            if terminates_transaction {
                break
            }
        }

        Ok(results)
    }

    pub fn execute_only_one(
        &self,
        context: &Context,
        query: &str
    ) -> Result<StatementResult, SimpleDbError>{
        let mut parser = Parser::create(query.to_string());
        let statement = parser.next_statement()?.unwrap();
        self.statement_executor.execute(&context, statement)
    }
}

impl Context {
    pub fn empty() -> Context {
        Context {
            transaction: None,
            database: None
        }
    }

    pub fn with_database(name: &str) -> Context {
        Context {
            database: Some(name.to_string()),
            transaction: None,
        }
    }

    pub fn has_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    pub fn has_database(&self) -> bool {
        self.database.is_some()
    }

    pub fn with(name: &str, transaction: Transaction) -> Context {
        Context {
            database: Some(name.to_string()),
            transaction: Some(transaction),
        }
    }

    pub fn database(&self) -> &String {
        self.database.as_ref().unwrap()
    }

    pub fn transaction(&self) -> &Transaction {
        self.transaction.as_ref().unwrap()
    }
}

impl StatementResult {
    pub fn get_transaction(self) -> Transaction {
        match self {
            StatementResult::TransactionStarted(transaction) => transaction,
            _ => panic!("")
        }
    }

    pub fn data(self) -> QueryIterator {
        match self {
            StatementResult::Data(data) => data,
            _ => panic!("")
        }
    }
}