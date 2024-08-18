use std::collections::HashSet;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use crossbeam_skiplist::SkipSet;
use crate::transactions::transaction::Transaction;

#[derive(Clone)]
pub enum IsolationLevel {
    ReadUncommited,
    SnapshotIsolation //MVCC
}

pub struct TransactionManager {
    active_transactions: SkipSet<u64>,
    next_txn_id: AtomicU64,
}

impl TransactionManager {
    pub fn new(next_txn_id: u64) -> TransactionManager {
        TransactionManager {
            next_txn_id: AtomicU64::new(next_txn_id),
            active_transactions: SkipSet::new(),
        }
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
}