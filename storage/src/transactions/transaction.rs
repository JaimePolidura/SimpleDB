use crate::transactions::transaction_manager::IsolationLevel;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::AtomicUsize;
use std::collections::HashSet;
use crossbeam_skiplist::{SkipList, SkipSet};
use shared::TxnId;
use crate::key::Key;

pub struct Transaction {
    pub(crate) active_transactions: HashSet<TxnId>,
    pub(crate) isolation_level: IsolationLevel,
    pub(crate) txn_id: TxnId,
}

impl Transaction {
    pub fn none() -> Transaction {
        Transaction {
            isolation_level: IsolationLevel::ReadUncommited,
            active_transactions: HashSet::new(),
            txn_id: 0
        }
    }

    pub fn create(id: TxnId) -> Transaction {
        Transaction {
            isolation_level: IsolationLevel::SnapshotIsolation,
            active_transactions: HashSet::new(),
            txn_id: id
        }
    }

    pub fn can_read(&self, key: &Key) -> bool {
        match self.isolation_level {
            IsolationLevel::SnapshotIsolation => {
                key.txn_id() <= self.txn_id && !self.active_transactions.contains(&key.txn_id())
            },
            IsolationLevel::ReadUncommited => true
        }
    }

    pub fn id(&self) -> TxnId {
        self.txn_id
    }
}

impl Clone for Transaction {
    fn clone(&self) -> Self {
        Transaction {
            active_transactions: self.active_transactions.clone(),
            isolation_level: self.isolation_level.clone(),
            txn_id: self.txn_id,
        }
    }
}