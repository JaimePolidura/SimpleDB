use crate::database::databases::Databases;
use crate::index::index_type::IndexType;
use crate::sql::parser::parser::Parser;
use crate::sql::query_iterator::QueryIterator;
use crate::sql::parser::statement::Statement;
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use crate::sql::StatementExecutor;
use crate::table::schema::Column;

pub struct SimpleDb {
    statement_executor: StatementExecutor,

    databases: Arc<Databases>,
}

pub enum StatementResult {
    TransactionStarted(Transaction),
    Data(QueryIterator),
    Ok(usize), //usize number of rows affected
    Databases(Vec<String>),
    Tables(Vec<String>),
    Describe(Vec<Column>),
    Indexes(Vec<(String, IndexType)>)
}

pub fn create(
    options: Arc<SimpleDbOptions>,
) -> Result<SimpleDb, SimpleDbError> {
    let databases = Arc::new(Databases::create(options.clone())?);
    
    Ok(SimpleDb {
        statement_executor: StatementExecutor::create(&databases),
        databases,
    })
}


#[derive(Clone)]
pub struct Context {
    transaction: Option<Transaction>,
    database: Option<String>,
}

impl SimpleDb {
    pub fn parse(
        &self,
        statement: &str
    ) -> Result<Statement, SimpleDbError> {
        let mut parser = Parser::create(statement.to_string());
        let statement = parser.next_statement()?.unwrap();
        Ok(statement)
    }

    pub fn execute(
        &self,
        context: &Context,
        statement: Statement
    ) -> Result<StatementResult, SimpleDbError> {
        self.statement_executor.execute(&context, statement)
    }

    pub fn get_databases(&self) -> Arc<Databases> {
        self.databases.clone()
    }
}

impl Context {
    pub fn empty() -> Context {
        Context {
            transaction: None,
            database: None
        }
    }

    pub fn create_with_database(name: &str) -> Context {
        Context {
            database: Some(name.to_string()),
            transaction: None,
        }
    }

    pub fn create(name: &str, transaction: Transaction) -> Context {
        Context {
            database: Some(name.to_string()),
            transaction: Some(transaction),
        }
    }

    pub fn clear_transaction(&mut self) -> Transaction {
        self.transaction.take().unwrap()
    }

    pub fn with_transaction(&mut self, transaction: Transaction) {
        self.transaction = Some(transaction);
    }

    pub fn with_database(&mut self, database: &str) {
        self.database = Some(database.to_string());
    }

    pub fn has_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    pub fn has_database(&self) -> bool {
        self.database.is_some()
    }

    pub fn database(&self) -> &String {
        self.database.as_ref().unwrap()
    }

    pub fn transaction(&self) -> &Transaction {
        self.transaction.as_ref().unwrap()
    }
}

impl StatementResult {
    pub fn get_transaction(&self) -> Transaction {
        match self {
            StatementResult::TransactionStarted(transaction) => transaction.clone(),
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