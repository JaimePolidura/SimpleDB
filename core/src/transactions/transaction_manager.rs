use crate::transactions::transaction_log::{TransactionLog, TransactionLogEntry};
use crate::transactions::transaction::{Transaction, TxnId};
use std::sync::atomic::Ordering::Relaxed;
use crate::lsm_options::LsmOptions;
use std::sync::atomic::AtomicU64;
use crossbeam_skiplist::SkipSet;
use crate::lsm_error::LsmError;
use std::collections::HashSet;
use std::sync::Arc;
use std::cmp::max;

#[derive(Clone)]
pub enum IsolationLevel {
    ReadUncommited,
    SnapshotIsolation //MVCC
}

pub struct TransactionManager {
    log: TransactionLog,

    active_transactions: SkipSet<TxnId>,
    next_txn_id: AtomicU64,
}

impl TransactionManager {
    pub fn create_recover_from_log(options: Arc<LsmOptions>) -> Result<TransactionManager, LsmError> {
        let mut transaction_log = TransactionLog::create(options)?;
        let transaction_log_entries = transaction_log.read_entries()?;
        let (open_transactions, max_txn_id) = Self::get_active_transactions(&transaction_log_entries);

        transaction_log.replace_entries(&open_transactions)?;

        Ok(TransactionManager {
            active_transactions: SkipSet::from_iter(open_transactions.iter().map(|i| *i)),
            next_txn_id: AtomicU64::new((max_txn_id + 1) as u64),
            log: transaction_log,
        })
    }

    pub fn commit(&self, transaction: Transaction) {
        self.active_transactions.remove(&transaction.txn_id);
    }

    pub fn start_transaction(&self, isolation_level: IsolationLevel) -> Transaction {
        let active_transactions = self.copy_active_transactions();
        let txn_id = self.next_txn_id.fetch_add(1, Relaxed) as TxnId;
        self.active_transactions.insert(txn_id);

        Transaction {
            active_transactions,
            isolation_level,
            txn_id
        }
    }

    fn copy_active_transactions(&self) -> HashSet<TxnId> {
        let mut active_transactions: HashSet<TxnId> = HashSet::new();

        for atctive_transactions in &self.active_transactions {
            active_transactions.insert(*atctive_transactions.value());
        }

        active_transactions
    }

    fn get_active_transactions(entries: &Vec<TransactionLogEntry>) -> (Vec<TxnId>, TxnId) {
        let mut active_transactions: HashSet<TxnId> = HashSet::new();
        let mut max_txn_id: TxnId = 0;

        for entry in entries.iter() {
            max_txn_id = max(max_txn_id as usize, entry.txn_id());

            match entry {
                TransactionLogEntry::START(txn_id) => active_transactions.insert(*txn_id),
                TransactionLogEntry::COMMIT(txn_id) => active_transactions.remove(txn_id),
                TransactionLogEntry::ROLLBACK(txn_id) => active_transactions.remove(txn_id)
            };
        }

        (active_transactions.iter().map(|i| *i).collect(), max_txn_id)
    }
}