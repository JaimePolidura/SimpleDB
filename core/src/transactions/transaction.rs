use crate::transactions::transaction_manager::IsolationLevel;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::AtomicUsize;
use std::collections::HashSet;
use crate::key::Key;

pub type TxnId = usize;

pub struct Transaction {
    pub(crate) active_transactions: HashSet<TxnId>,
    pub(crate) isolation_level: IsolationLevel,
    pub(crate) n_not_compacted_writes: AtomicUsize,
    pub(crate) n_writes: AtomicUsize,
    pub(crate) txn_id: TxnId,
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

    pub(crate) fn increase_n_not_compacted_writes(&self) {
        self.n_not_compacted_writes.fetch_add(1, Relaxed);
    }

    pub(crate) fn increase_nwrites(&self) {
        self.n_writes.fetch_add(1, Relaxed);
    }

    pub(crate) fn all_writes_have_been_discarded(&self) -> bool {
        self.n_writes.load(Relaxed) == self.n_not_compacted_writes.load(Relaxed)
    }

    pub(crate) fn none() -> Transaction {
        Transaction {
            isolation_level: IsolationLevel::ReadUncommited,
            n_not_compacted_writes: AtomicUsize::new(0),
            active_transactions: HashSet::new(),
            n_writes: AtomicUsize::new(0),
            txn_id: 0
        }
    }
}

impl Clone for Transaction {
    fn clone(&self) -> Self {
        Transaction {
            n_not_compacted_writes: AtomicUsize::new(self.n_not_compacted_writes.load(Relaxed)),
            n_writes: AtomicUsize::new(self.n_writes.load(Relaxed)),
            active_transactions: self.active_transactions.clone(),
            isolation_level: self.isolation_level.clone(),
            txn_id: self.txn_id,
        }
    }
}