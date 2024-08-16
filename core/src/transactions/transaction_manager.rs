use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

pub struct TransactionManager {
    next_txn_id: AtomicU64,
}

pub struct Transaction {
    txn_id: u64,
}

impl TransactionManager {
    pub fn new(next_txn_id: u64) -> TransactionManager {
        TransactionManager {
            next_txn_id: AtomicU64::new(next_txn_id),
        }
    }

    pub fn start_transaction(&self) -> Transaction {
        Transaction { txn_id: self.next_txn_id() }
    }

    pub fn next_txn_id(&self) -> u64 {
        self.next_txn_id.fetch_add(1, Relaxed)
    }
}