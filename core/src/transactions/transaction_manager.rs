use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use crossbeam_skiplist::SkipSet;
use crate::lsm_error::LsmError;
use crate::lsm_options::LsmOptions;
use crate::transactions::transaction::Transaction;
use crate::transactions::transaction_log::{TransactionLog, TransactionLogEntry};

#[derive(Clone)]
pub enum IsolationLevel {
    ReadUncommited,
    SnapshotIsolation //MVCC
}

pub struct TransactionManager {
    log: TransactionLog,

    active_transactions: SkipSet<u64>,
    next_txn_id: AtomicU64,
}

impl TransactionManager {
    pub fn create_recover_from_log(&self, options: Arc<LsmOptions>) -> Result<TransactionManager, LsmError> {
        let mut transaction_log = TransactionLog::create(options)?;
        let transaction_log_entries = transaction_log.read_entries()?;
        let (open_transactions, max_txn_id) = Self::get_active_transactions(&transaction_log_entries);

        transaction_log.replace_entries(&open_transactions)?;

        Ok(TransactionManager{
            active_transactions: SkipSet::from_iter(open_transactions.iter()
                .map(|i| i.txn_id())),
            next_txn_id: max_txn_id + 1,
            log: transaction_log,
        })
    }

    pub fn commit(&self, transaction: Transaction) {
        self.active_transactions.remove(&transaction.txn_id);
    }

    pub fn start_transaction(&self, isolation_level: IsolationLevel) -> Transaction {
        let active_transactions = self.copy_active_transactions();
        let txn_id = self.next_txn_id.fetch_add(1, Relaxed);
        self.active_transactions.insert(txn_id);

        Transaction {
            active_transactions,
            isolation_level,
            txn_id
        }
    }

    fn copy_active_transactions(&self) -> HashSet<u64> {
        unimplemented!();
    }

    fn get_active_transactions(entries: &Vec<TransactionLogEntry>) -> (Vec<TransactionLogEntry>, usize) {
        unimplemented!();
    }
}