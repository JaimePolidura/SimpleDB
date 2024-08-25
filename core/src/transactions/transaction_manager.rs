use crate::transactions::transaction_log::{TransactionLog, TransactionLogEntry};
use crate::transactions::transaction::{Transaction, TxnId};
use std::sync::atomic::{AtomicU64, AtomicUsize};
use crossbeam_skiplist::{SkipMap, SkipSet};
use std::sync::atomic::Ordering::Relaxed;
use crate::lsm_options::LsmOptions;
use crate::lsm_error::LsmError;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use crate::key::Key;
use std::cmp::max;

#[derive(Clone)]
pub enum IsolationLevel {
    ReadUncommited,
    SnapshotIsolation //MVCC
}

pub struct TransactionManager {
    rolledback_transactions: SkipMap<TxnId, Arc<Transaction>>,
    active_transactions: SkipSet<TxnId>,
    next_txn_id: AtomicU64,
    log: TransactionLog,
}

impl TransactionManager {
    pub fn create_recover_from_log(options: Arc<LsmOptions>) -> Result<TransactionManager, LsmError> {
        let mut log = TransactionLog::create(options)?;
        let transaction_log_entries = log.read_entries()?;
        let (active_transactions, max_txn_id) = Self::get_active_transactions(&transaction_log_entries);
        let rolledback_transactions = Self::get_pending_transactions_to_rollback(&transaction_log_entries);

        Ok(TransactionManager {
            next_txn_id: AtomicU64::new((max_txn_id + 1) as u64),
            rolledback_transactions,
            active_transactions,
            log,
        })
    }

    pub fn create_mock(options: Arc<LsmOptions>) -> TransactionManager {
        TransactionManager{
            log: TransactionLog::create_mock(options),
            rolledback_transactions: SkipMap::new(),
            active_transactions: SkipSet::new(),
            next_txn_id: AtomicU64::new(0),
        }
    }

    pub fn commit(&self, transaction: Transaction) {
        self.active_transactions.remove(&transaction.txn_id);
        self.log.add_entry(TransactionLogEntry::Commit(transaction.txn_id));
    }

    pub fn rollback(&self, transaction: Transaction) {
        self.log.add_entry(TransactionLogEntry::StartRollback(transaction.txn_id, transaction.n_writes.load(Relaxed)));
        self.rolledback_transactions.insert(transaction.txn_id, Arc::new(transaction));
    }

    pub fn check_key_not_rolledback(&self, key: &Key) -> Result<(), ()> {
        match self.rolledback_transactions.get(&key.txn_id())
            .map(|entry| entry.value().clone()) {

            Some(rolledback_transaction) => {
                rolledback_transaction.increase_n_writes_rolledback();

                if rolledback_transaction.all_writes_have_been_rolledback() {
                    self.rolledback_transactions.remove(&rolledback_transaction.txn_id);
                }
                Err(())
            }
            None => Ok(())
        }
    }

    pub fn start_transaction(&self, isolation_level: IsolationLevel) -> Transaction {
        let active_transactions = self.copy_active_transactions();
        let txn_id = self.next_txn_id.fetch_add(1, Relaxed) as TxnId;
        self.active_transactions.insert(txn_id);

        Transaction {
            n_writes_rolled_back: AtomicUsize::new(0),
            n_writes: AtomicUsize::new(0),
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

    fn get_pending_transactions_to_rollback(entries: &Vec<TransactionLogEntry>) -> SkipMap<TxnId, Arc<Transaction>> {
        let mut rolledback_transactions: SkipMap<TxnId, Arc<Transaction>> = SkipMap::new();

        for entry in entries.iter() {
            match entry {
                TransactionLogEntry::StartRollback(txn_id, n_writes) => {
                    rolledback_transactions.insert(*txn_id, Arc::new(Transaction{
                        isolation_level: IsolationLevel::SnapshotIsolation, //These two fields doest matter
                        active_transactions: HashSet::new(),
                        n_writes_rolled_back: AtomicUsize::new(0),
                        n_writes: AtomicUsize::new(*n_writes),
                        txn_id: *txn_id
                    }));
                },
                TransactionLogEntry::RolledbackWrite(txn_id) => {
                    let entry = rolledback_transactions.get(txn_id).unwrap();
                    let transaction = entry.value();
                    transaction.increase_n_writes_rolledback();
                    if transaction.all_writes_have_been_rolledback() {
                        rolledback_transactions.remove(txn_id);
                    }
                },
                _ => {  },
            };
        }

        rolledback_transactions
    }

    fn get_active_transactions(entries: &Vec<TransactionLogEntry>) -> (SkipSet<TxnId>, TxnId) {
        let mut active_transactions: SkipSet<TxnId> = SkipSet::new();
        let mut max_txn_id: TxnId = 0;

        for entry in entries.iter() {
            max_txn_id = max(max_txn_id as usize, entry.txn_id());

            match *entry {
                TransactionLogEntry::Start(txn_id) => { active_transactions.insert(txn_id); },
                TransactionLogEntry::Commit(txn_id) => { active_transactions.remove(&txn_id); },
                TransactionLogEntry::StartRollback(txn_id, _) => { active_transactions.remove(&txn_id); },
                TransactionLogEntry::RolledbackWrite(_) => { }
            };
        }

        (active_transactions, max_txn_id)
    }
}