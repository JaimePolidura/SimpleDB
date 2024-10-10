use crate::transactions::transaction::Transaction;
use crate::transactions::transaction_log::{TransactionLog, TransactionLogEntry};
use crossbeam_skiplist::SkipMap;
use shared::{SimpleDbError, TxnId};
use std::cmp::max;
use std::collections::HashSet;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use shared::key::Key;

#[derive(Clone)]
pub enum IsolationLevel {
    ReadUncommited,
    SnapshotIsolation //MVCC
}

pub struct TransactionManager {
    rolledback_transactions: SkipMap<TxnId, AtomicUsize>,
    active_transactions: SkipMap<TxnId, AtomicUsize>,
    next_txn_id: AtomicU64,
    log: TransactionLog,
}

impl TransactionManager {
    pub fn create_recover_from_log(options: Arc<shared::SimpleDbOptions>) -> Result<TransactionManager, shared::SimpleDbError> {
        let log = TransactionLog::create(options)?;
        let transaction_log_entries = log.read_entries()?;
        let (active_transactions, pending_to_rollback, max_txn_id) =
            Self::get_pending_transactions(&transaction_log_entries);

        let mut new_log_entries = Vec::new();
        new_log_entries.extend(Self::pending_transactions_to_log_entries(&active_transactions));
        new_log_entries.extend(Self::pending_transactions_to_log_entries(&pending_to_rollback));
        log.replace_entries(&new_log_entries)?;

        Ok(TransactionManager {
            rolledback_transactions: Self::pending_transactions_to_txnids(&active_transactions, &pending_to_rollback),
            next_txn_id: AtomicU64::new((max_txn_id + 1) as u64),
            active_transactions: SkipMap::new(),
            log,
        })
    }

    pub fn get_active_transactions(&self) -> Vec<shared::TxnId> {
        let mut active_transactions = Vec::new();

        for active_transaction_id in &self.active_transactions {
            active_transactions.push(*active_transaction_id.key());
        }

        active_transactions
    }

    pub fn create_mock(options: Arc<shared::SimpleDbOptions>) -> TransactionManager {
        TransactionManager {
            log: TransactionLog::create_mock(options),
            rolledback_transactions: SkipMap::new(),
            active_transactions: SkipMap::new(),
            next_txn_id: AtomicU64::new(0),
        }
    }

    pub fn commit(&self, transaction: &Transaction) -> Result<(), SimpleDbError> {
        self.active_transactions.remove(&transaction.txn_id);
        self.log.add_entry(TransactionLogEntry::Commit(transaction.txn_id))
    }

    //Before calling this function it is expected that the written keys have been removed
    pub fn rollback(&self, transaction: &Transaction) -> Result<(), SimpleDbError> {
        self.log.add_entry(TransactionLogEntry::StartRollback(transaction.txn_id))?;
        let n_writes = self.active_transactions.get(&transaction.txn_id)
            .unwrap()
            .value()
            .load(Relaxed);
        self.active_transactions.remove(&transaction.txn_id);

        if n_writes > 0 {
            self.rolledback_transactions.insert(transaction.txn_id, AtomicUsize::new(n_writes));
        }

        Ok(())
    }

    //This function is called when there is a memtable flush or sstable compaction
    //Returns Ok if the key with that transaction ID hasn't been rolledback
    //Returns Err if it has been rolledback
    pub fn on_write_key(&self, key: &Key) -> Result<(), ()> {
        match self.rolledback_transactions.get(&key.txn_id()) {
            Some(entry_rolledback_transaction) => {
                let n_writes_entry_rolledback_transaction = entry_rolledback_transaction.value();

                let _ = self.log.add_entry(TransactionLogEntry::RolledbackWrite(key.txn_id()));
                n_writes_entry_rolledback_transaction.fetch_sub(1, Relaxed);

                if n_writes_entry_rolledback_transaction.load(Relaxed) == 0 {
                    self.rolledback_transactions.remove(&key.txn_id());
                }
                Err(())
            }
            None => Ok(())
        }
    }

    pub fn start_transaction(&self, isolation_level: IsolationLevel) -> Transaction {
        let active_transactions = self.copy_active_transactions();
        let txn_id = self.next_txn_id.fetch_add(1, Relaxed) as shared::TxnId;
        self.active_transactions.insert(txn_id, AtomicUsize::new(0));

        Transaction {
            active_transactions,
            isolation_level,
            txn_id
        }
    }

    pub fn mark_write(&self, transaction: &Transaction) -> Result<(), SimpleDbError> {
        match self.active_transactions.get(&transaction.txn_id) {
            Some(n_writes) => {
                self.log.add_entry(TransactionLogEntry::Write(transaction.txn_id))?;
                n_writes.value().fetch_add(1, Relaxed);
                Ok(())
            }
            None => Ok(())
        }
    }

    pub fn is_active(&self, txn_id: TxnId) -> bool {
        self.active_transactions.get(&txn_id).is_some()
    }

    fn copy_active_transactions(&self) -> HashSet<TxnId> {
        let mut active_transactions: HashSet<TxnId> = HashSet::new();

        for active_transaction in &self.active_transactions {
            active_transactions.insert(*active_transaction.key());
        }

        active_transactions
    }

    fn get_pending_transactions(
        entries: &Vec<TransactionLogEntry>
    ) -> (SkipMap<TxnId, usize>, SkipMap<TxnId, usize>, TxnId) {
        //Transaction ID -> Nº writes
        let active_transactions = SkipMap::new();
        let transactions_to_rollback = SkipMap::new();
        let mut max_txn_id = 1 as TxnId;

        for entry in entries.iter() {
            max_txn_id = max(entry.txn_id(), max_txn_id);

            match entry {
                TransactionLogEntry::StartRollback(txn_id) => {
                    if let Some(n_writes_to_rollback_entry) = active_transactions.get(txn_id) {
                        let n_writes_to_rollback = *n_writes_to_rollback_entry.value();
                        transactions_to_rollback.insert(*txn_id, n_writes_to_rollback);
                        active_transactions.remove(txn_id);
                    }
                },
                TransactionLogEntry::Write(txn_id) => {
                    if !active_transactions.contains_key(txn_id) {
                        active_transactions.insert(*txn_id, 0);
                    }
                    active_transactions.insert(*txn_id, active_transactions.get(txn_id).unwrap().value() + 1);
                },
                TransactionLogEntry::Commit(txn_id) => {
                    active_transactions.remove(txn_id);
                }
                TransactionLogEntry::RolledbackWrite(txn_id) => {
                    if transactions_to_rollback.contains_key(txn_id) {
                        let new_nwrites = transactions_to_rollback.get(txn_id).unwrap().value() - 1;
                        if new_nwrites > 0 {
                            transactions_to_rollback.insert(
                                *txn_id, transactions_to_rollback.get(txn_id).unwrap().value() - 1
                            );
                        } else {
                            transactions_to_rollback.remove(txn_id);
                        }
                    }
                },
                _ => {  },
            };
        }

        (active_transactions, transactions_to_rollback, max_txn_id)
    }

    fn pending_transactions_to_txnids(
        rollback_transaction: &SkipMap<TxnId, usize>,
        active_transaction: &SkipMap<TxnId, usize>,
    ) -> SkipMap<TxnId, AtomicUsize> {
        let txnids = SkipMap::new();
        for entry in rollback_transaction.iter() {
            txnids.insert(*entry.key(), AtomicUsize::new(*entry.value()));
        }
        for entry in active_transaction.iter() {
            txnids.insert(*entry.key(), AtomicUsize::new(*entry.value()));
        }

        txnids
    }

    fn pending_transactions_to_log_entries(
        pending_transaction: &SkipMap<TxnId, usize>
    ) -> Vec<TransactionLogEntry> {
        let mut entries = Vec::new();

        for entry in pending_transaction.iter() {
            let n_writes = *entry.value() as u32;
            let txn_id = *entry.key();

            for _ in 0..n_writes {
                entries.push(TransactionLogEntry::Write(txn_id));
            }
            entries.push(TransactionLogEntry::StartRollback(txn_id));
            entries.push(TransactionLogEntry::Write(txn_id));
        }

        entries
    }
}