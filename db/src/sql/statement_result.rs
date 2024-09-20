use storage::transactions::transaction::Transaction;

pub enum StatementResult {
    TransactionStarted(Transaction),
    Ok(usize), //usize number of rows affected
}