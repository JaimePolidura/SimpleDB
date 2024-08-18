use std::collections::HashSet;
use crate::key::Key;
use crate::transactions::transaction_manager::IsolationLevel;

#[derive(Clone)]
pub struct Transaction {
    pub(crate) active_transactions: HashSet<u64>,
    pub(crate) isolation_level: IsolationLevel,
    pub(crate) txn_id: u64,
}

impl Transaction {
    pub(crate) fn can_read(&self, key: &Key) -> bool {
        match self.isolation_level {
            IsolationLevel::SnapshotIsolation => {
                key.txn_id() <= self.txn_id && !self.active_transactions.contains(&key.txn_id())
            },
            IsolationLevel::ReadUncommited => true
        }
    }

    pub(crate) fn none() -> Transaction {
        Transaction {
            active_transactions: HashSet::new(),
            isolation_level: IsolationLevel::ReadUncommited,
            txn_id: 0
        }
    }
}