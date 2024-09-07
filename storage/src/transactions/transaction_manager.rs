use crate::key::Key;
use crate::lsm_error::LsmError;
use crate::transactions::transaction::{Transaction, TxnId};
use crate::transactions::transaction_log::{TransactionLog, TransactionLogEntry};
use crossbeam_skiplist::{SkipMap, SkipSet};
use std::cmp::max;
use std::collections::HashSet;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;

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
    pub fn create_recover_from_log(options: Arc<shared::SimpleDbOptions>) -> Result<TransactionManager, LsmError> {
        let mut log = TransactionLog::create(options)?;
        let transaction_log_entries = log.read_entries()?;
        let rolledback_transactions = Self::get_pending_transactions_to_rollback_from_log_entries(&transaction_log_entries);
        let (active_transactions, max_txn_id) = Self::get_active_transactions_from_log_entries(&transaction_log_entries);

        log.replace_entries(&Self::create_new_transaction_log_entries(&rolledback_transactions, &active_transactions))?;

        Ok(TransactionManager {
            next_txn_id: AtomicU64::new((max_txn_id + 1) as u64),
            rolledback_transactions,
            active_transactions,
            log,
        })
    }

    pub fn get_active_transactions(&self) -> Vec<TxnId> {
        let mut active_transactions = Vec::new();

        for active_transaction_id in &self.active_transactions {
            active_transactions.push(*active_transaction_id.value());
        }

        active_transactions
    }

    pub fn create_mock(options: Arc<shared::SimpleDbOptions>) -> TransactionManager {
        TransactionManager {
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

    pub fn rollback_active_transaction_failure(&self, txn_id: TxnId) -> Result<(), LsmError> {
        self.log.add_entry(TransactionLogEntry::RolledbackActiveTransactionFailure(txn_id))?;
        self.active_transactions.remove(&txn_id);
        Ok(())
    }

    //A rolledback transaction won't be removed from active transactions, so no other transaction can see its changes
    //Once all writes has been discareded in compaction or memtable flush, it wil be removed.
    pub fn rollback(&self, transaction: Transaction) {
        self.log.add_entry(TransactionLogEntry::StartRollback(transaction.txn_id, transaction.n_writes.load(Relaxed)));
        self.rolledback_transactions.insert(transaction.txn_id, Arc::new(transaction));
    }

    //This function is called when there is a memtable flush or sstable compaction
    //Returns Ok if the key with that transaction ID hasn't been rolledback
    //Returns Err if it has been rolledback
    pub fn on_write_key(&self, key: &Key) -> Result<(), ()> {
        match self.rolledback_transactions.get(&key.txn_id())
            .map(|entry| entry.value().clone()) {

            Some(rolledback_transaction) => {
                self.log.add_entry(TransactionLogEntry::RolledbackWrite(rolledback_transaction.txn_id));
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

    pub fn is_active(&self, txn_id: TxnId) -> bool {
        self.active_transactions.contains(&txn_id)
    }

    fn copy_active_transactions(&self) -> HashSet<TxnId> {
        let mut active_transactions: HashSet<TxnId> = HashSet::new();

        for atctive_transactions in &self.active_transactions {
            active_transactions.insert(*atctive_transactions.value());
        }

        active_transactions
    }

    fn get_pending_transactions_to_rollback_from_log_entries(entries: &Vec<TransactionLogEntry>) -> SkipMap<TxnId, Arc<Transaction>> {
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
                TransactionLogEntry::RolledbackActiveTransactionFailure(txn_id ) => {
                    rolledback_transactions.remove(txn_id);
                },
                _ => {  },
            };
        }

        rolledback_transactions
    }

    fn get_active_transactions_from_log_entries(entries: &Vec<TransactionLogEntry>) -> (SkipSet<TxnId>, TxnId) {
        let mut active_transactions: SkipSet<TxnId> = SkipSet::new();
        let mut max_txn_id: TxnId = 0;

        for entry in entries.iter() {
            max_txn_id = max(max_txn_id as usize, entry.txn_id());

            match *entry {
                TransactionLogEntry::RolledbackActiveTransactionFailure(txn_id) => { active_transactions.remove(&txn_id); }
                TransactionLogEntry::Start(txn_id) => { active_transactions.insert(txn_id); },
                TransactionLogEntry::Commit(txn_id) => { active_transactions.remove(&txn_id); },
                TransactionLogEntry::StartRollback(txn_id, _) => { active_transactions.remove(&txn_id); },
                TransactionLogEntry::RolledbackWrite(_) => { }
            };
        }

        (active_transactions, max_txn_id)
    }


    fn create_new_transaction_log_entries(
        rolledback_transactions: &SkipMap<TxnId, Arc<Transaction>>,
        active_transactions: &SkipSet<TxnId>,
    ) -> Vec<TransactionLogEntry> {
        let mut entries: Vec<TransactionLogEntry> = Vec::new();

        for active_txn_id in active_transactions {
            entries.push(TransactionLogEntry::Start(*active_txn_id.value()));
        }

        for rolledback_transaction in rolledback_transactions.iter() {
            let rolledback_transaction = rolledback_transaction.value();
            let pending_writes_to_rollback = rolledback_transaction.get_pending_writes_to_rollback();

            if pending_writes_to_rollback > 0 {
                entries.push(TransactionLogEntry::StartRollback(rolledback_transaction.txn_id, pending_writes_to_rollback));
            }
        }

        entries
    }
}