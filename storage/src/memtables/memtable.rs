use crate::key;
use crate::key::Key;
use crate::memtables::memtable::MemtableState::{Active, Flushed, Flusing, Inactive, RecoveringFromWal};
use crate::memtables::wal::Wal;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::transactions::transaction::{Transaction};
use crate::transactions::transaction_manager::TransactionManager;
use crate::utils::storage_iterator::StorageIterator;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::{SkipMap, SkipSet};
use std::cell::UnsafeCell;
use std::ops::Bound::Excluded;
use std::ops::Shl;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

const TOMBSTONE: Bytes = Bytes::new();

pub struct MemTable {
    data: Arc<SkipMap<Key, Bytes>>,
    current_size_bytes: AtomicUsize,
    max_size_bytes: usize,
    memtable_id: shared::MemtableId,
    state: UnsafeCell<MemtableState>,
    wal: UnsafeCell<Wal>,
    options: Arc<shared::SimpleDbOptions>,
    txn_ids_written: SkipSet<shared::TxnId>,
    keyspace_id: shared::KeyspaceId,
}

enum MemtableState {
    New,
    RecoveringFromWal,
    Active,
    Inactive,
    Flusing,
    Flushed
}

impl MemTable {
    pub fn create_new(
        options: Arc<shared::SimpleDbOptions>,
        memtable_id: shared::MemtableId,
        keyspace_id: shared::KeyspaceId
    ) -> Result<MemTable, shared::SimpleDbError> {
        Ok(MemTable {
            wal: UnsafeCell::new(Wal::create(options.clone(), keyspace_id, memtable_id)?),
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::New),
            data: Arc::new(SkipMap::new()),
            txn_ids_written: SkipSet::new(),
            keyspace_id,
            memtable_id,
            options,
        })
    }

    pub fn create_mock(
        options: Arc<shared::SimpleDbOptions>,
        memtable_id: shared::MemtableId
    ) -> Result<MemTable, shared::SimpleDbError> {
        Ok(MemTable {
            wal: UnsafeCell::new(Wal::create_mock(options.clone(), memtable_id)?),
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::Active),
            data: Arc::new(SkipMap::new()),
            txn_ids_written: SkipSet::new(),
            keyspace_id: 0,
            memtable_id,
            options,
        })
    }

    pub fn create_and_recover_from_wal(
        options: Arc<shared::SimpleDbOptions>,
        memtable_id: shared::MemtableId,
        keyspace_id: shared::KeyspaceId,
        wal: Wal
    ) -> Result<MemTable, shared::SimpleDbError> {
        let mut memtable = MemTable {
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::New),
            data: Arc::new(SkipMap::new()),
            wal: UnsafeCell::new(wal),
            txn_ids_written: SkipSet::new(),
            keyspace_id,
            memtable_id,
            options
        };

        memtable.recover_from_wal();

        Ok(memtable)
    }

    pub fn has_txn_id_been_written(&self, txn_id: shared::TxnId) -> bool {
        self.txn_ids_written.contains(&txn_id)
    }

    pub fn set_inactive(&self) {
        unsafe { (* self.state.get()) = Inactive; }
    }

    pub fn set_active(&self) {
        unsafe { (* self.state.get()) = Active; }
    }

    pub fn set_flushing(&self) {
        unsafe { (* self.state.get()) = Flusing; }
    }

    pub fn set_recovering_from_wal(&self) {
        unsafe { (* self.state.get()) = RecoveringFromWal; }
    }

    pub fn set_flushed(&self) {
        unsafe {
            (* self.state.get()) = Flushed;
            (*self.wal.get()).delete_wal().expect("Cannot delete WAL");
        }
    }

    pub fn get_id(&self) -> shared::MemtableId {
        self.memtable_id
    }

    pub fn get(&self, key_lookup: &Bytes, transaction: &Transaction) -> Option<Bytes> {
        let mut current_key = key::create(key_lookup.clone(), transaction.txn_id + 1);

        loop {
            if let Some(entry) = self.data.upper_bound(Excluded(&current_key)) {
                if !entry.key().bytes_eq_bytes(&key_lookup) {
                    return None;
                }
                if transaction.can_read(entry.key()) {
                    return Some(entry.value().clone());
                }

                current_key = entry.key().clone();
            } else {
                return None;
            }
        }
    }

    pub fn set(&self, transaction: &Transaction, key: Bytes, value: &[u8]) -> Result<(), shared::SimpleDbError> {
        self.write_into_skip_list(
            &key::create(key, transaction.txn_id),
            Bytes::copy_from_slice(value),
            transaction.txn_id
        )
    }

    pub fn delete(&self, transaction: &Transaction, key: Bytes) -> Result<(), shared::SimpleDbError> {
        self.write_into_skip_list(
            &key::create(key, transaction.txn_id),
            TOMBSTONE,
            transaction.txn_id
        )
    }

    fn write_into_skip_list(&self, key: &Key, value: Bytes, txn_id: shared::TxnId) -> Result<(), shared::SimpleDbError> {
        if !self.can_memtable_be_written() {
            return Ok(());
        }
        if self.current_size_bytes.load(Relaxed) >= self.max_size_bytes {
            return Err(shared::SimpleDbError::Internal);
        }

        self.write_wal(&key, &value)?;

        self.txn_ids_written.insert(txn_id);

        self.current_size_bytes.fetch_add(key.len() + value.len(), Relaxed);

        self.data.insert(key.clone(), value);

        Ok(())
    }

    fn write_wal(&self, key: &Key, value: &Bytes) -> Result<(), shared::SimpleDbError> {
        //Multiple threads can write to the WAL concurrently, since the kernel already makes sure
        //that there won't be race conditions when multiple threads are writing to an append only file
        //https://nullprogram.com/blog/2016/08/03/
        let wal: &mut Wal = unsafe { &mut *self.wal.get() };

        if self.can_memtable_wal_be_written() {
            wal.add_entry(key, value)
        } else {
            Ok(())
        }
    }

    pub fn to_sst(self: &Arc<MemTable>, transaction_manager: &Arc<TransactionManager>) -> SSTableBuilder {
        let mut memtable_iterator = MemtableIterator::create(&self, &Transaction::none());
        let mut sstable_builder = SSTableBuilder::create(self.options.clone(),
                                                         transaction_manager.clone(), self.keyspace_id, 0);
        sstable_builder.set_memtable_id(self.memtable_id);

        while memtable_iterator.next() {
            let value = memtable_iterator.value();
            let key = memtable_iterator.key();

            match transaction_manager.on_write_key(key) {
                Ok(_) => sstable_builder.add_entry(key.clone(), Bytes::copy_from_slice(value)),
                Err(_) => {}
            };
        }

        sstable_builder
    }

    fn recover_from_wal(&mut self) -> Result<(), shared::SimpleDbError> {
        self.set_recovering_from_wal();
        let wal: &Wal = unsafe { &*self.wal.get() };
        let mut entries = wal.read_entries()?;

        println!("Applying {} operations from WAL to memtable with ID: {}", entries.len(), wal.get_memtable_id());

        while let Some(entry) = entries.pop() {
            self.write_into_skip_list(&entry.key, entry.value, entry.key.txn_id());
        }

        self.set_active();

        Ok(())
    }

    fn can_memtable_be_written(&self) -> bool {
        let current_state = unsafe { &*self.state.get() };

        match current_state {
            Active | RecoveringFromWal => true,
            _ => false,
        }
    }

    fn can_memtable_wal_be_written(&self) -> bool {
        let current_state = unsafe { &*self.state.get() };
        match current_state {
            MemtableState::Active => true,
            _ => false,
        }
    }
}

//This iterators fulfills:
// - The returned keys are readble/visible by the current transaction.
// - The returned key's bytes might be returned multiple times.
//
//   For example (byess, txn_id): (A, 1), (A, 2), (A, 3) with iterator txn_id = 2,
//   the iterator will return: (A, 1) and (A, 2)
pub struct MemtableIterator {
    memtable: Arc<MemTable>,

    current_value: Option<Bytes>,
    current_key: Option<Key>,

    transaction: Transaction,
}

impl MemtableIterator {
    pub fn create(memtable: &Arc<MemTable>, transaction: &Transaction) -> MemtableIterator {
        MemtableIterator {
            transaction: transaction.clone(),
            memtable: memtable.clone(),
            current_value: None,
            current_key: None,
        }
    }

    fn get_next_readable_key(&self) -> Option<(Key, Bytes)> {
        let mut current_key = self.current_key.clone();

        loop {
            match self.get_next_key(&current_key) {
                Some((key, value)) => {
                    if self.transaction.can_read(&key) {
                        return Some((key, value));
                    } else {
                        current_key = Some(key);
                    }
                },
                None => {
                    return None
                }
            }
        }
    }

    fn get_next_key(&self, prev_key: &Option<Key>) -> Option<(Key, Bytes)> {
        if self.memtable.data.is_empty() {
            return None;
        }

        match prev_key {
            Some(current_key) => {
                if let Some(entry) = self.memtable.data.lower_bound(Excluded(&current_key)) {
                    Some((entry.key().clone(), entry.value().clone()))
                } else {
                    None
                }
            },
            None => {
                let entry = self.memtable.data.iter().next()
                    .unwrap();
                Some((entry.key().clone(), entry.value().clone()))
            },
        }
    }
}

impl<'a> StorageIterator for MemtableIterator {
    fn next(&mut self) -> bool {
        match self.get_next_readable_key() {
            Some((next_key, next_value)) => {
                self.current_key = Some(next_key);
                self.current_value = Some(next_value);
                return true;
            },
            None => {
                return false;
            }
        }
    }

    fn has_next(&self) -> bool {
        self.get_next_readable_key().is_some()
    }

    fn key(&self) -> &Key {
        self.current_key
            .as_ref()
            .expect("Illegal iterator state")
    }

    fn value(&self) -> &[u8] {
        self.current_value
            .as_ref()
            .expect("Illegal iterator state")
    }
}

#[cfg(test)]
mod test {
    use crate::key;
    use crate::memtables::memtable::{MemTable, MemtableIterator};
    use crate::transactions::transaction::{Transaction};
    use crate::transactions::transaction_manager::IsolationLevel;
    use crate::utils::storage_iterator::StorageIterator;
    use std::sync::Arc;
    use bytes::Bytes;

    #[test]
    fn get_set_delete_no_transactions() {
        let memtable = MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0)
            .unwrap();
        let value: Vec<u8> = vec![10, 12];

        assert!(memtable.get(&Bytes::from("nombre"), &Transaction::none()).is_none());

        memtable.set(&Transaction::none(), Bytes::from("nombre"), &value);
        memtable.set(&Transaction::none(), Bytes::from("edad"), &value);

        assert!(memtable.get(&Bytes::from("nombre"), &Transaction::none()).is_some());
        assert!(memtable.get(&Bytes::from("edad"), &Transaction::none()).is_some());

        memtable.delete(&Transaction::none(), Bytes::from("nombre"));

        assert!(memtable.get(&Bytes::from("nombre"), &Transaction::none()).is_some());
    }

    #[test]
    fn get_set_delete_transactions() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0).unwrap());
        memtable.set_active();
        memtable.set(&transaction(10), Bytes::from("aa"), &vec![1]); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("alberto"), &vec![2]);
        memtable.set(&transaction(2), Bytes::from("alberto"), &vec![3]);
        memtable.set(&transaction(3), Bytes::from("alberto"), &vec![4]);
        memtable.set(&transaction(1), Bytes::from("gonchi"), &vec![5]);
        memtable.set(&transaction(5), Bytes::from("javier"), &vec![6]); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("jaime"), &vec![7]);
        memtable.set(&transaction(5), Bytes::from("jaime"), &vec![8]);
        memtable.set(&transaction(0), Bytes::from("wili"), &vec![9]);

        let to_test = memtable.get(&Bytes::from("alberto"), &transaction(2));
        assert!(to_test.is_some());
        assert!(to_test.unwrap().eq(&vec![3]));

        let to_test = memtable.get(&Bytes::from("aa"), &transaction(9));
        assert!(to_test.is_none());

        let to_test = memtable.get(&Bytes::from("jaime"), &transaction(6));
        assert!(to_test.is_some());
        assert!(to_test.unwrap().eq(&vec![8]));
    }

    #[test]
    fn iterators_readuncommited() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0).unwrap());
        memtable.set_active();
        let value: Vec<u8> = vec![10, 12];
        memtable.set(&transaction(1), Bytes::from("alberto"), &value);
        memtable.set(&transaction(2), Bytes::from("alberto"), &value);
        memtable.set(&transaction(3), Bytes::from("alberto"), &value);
        memtable.set(&transaction(1), Bytes::from("jaime"), &value);
        memtable.set(&transaction(5), Bytes::from("jaime"), &value);
        memtable.set(&transaction(1), Bytes::from("gonchi"), &value);
        memtable.set(&transaction(0), Bytes::from("wili"), &value);

        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("alberto", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("alberto", 2)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("alberto", 3)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("gonchi", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("jaime", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("jaime", 5)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("wili", 0)));

        assert!(!iterator.has_next());
    }

    #[test]
    fn iterators_snapshotisolation() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0).unwrap());
        memtable.set_active();
        let value: Vec<u8> = vec![10, 12];
        memtable.set(&transaction(10), Bytes::from("aa"), &value); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("alberto"), &value);
        memtable.set(&transaction(2), Bytes::from("alberto"), &value);
        memtable.set(&transaction(4), Bytes::from("alberto"), &value); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("gonchi"), &value);
        memtable.set(&transaction(5), Bytes::from("javier"), &value); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("jaime"), &value);
        memtable.set(&transaction(5), Bytes::from("jaime"), &value); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(0), Bytes::from("wili"), &value);

        let mut iterator = MemtableIterator::create(&memtable, &transaction_with_iso(3, IsolationLevel::SnapshotIsolation));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("alberto", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("alberto", 2)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("gonchi", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("jaime", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("wili", 0)));
    }

    fn transaction(txn_id: shared::TxnId) -> Transaction {
        let mut transaction = Transaction::none();
        transaction.txn_id = txn_id;
        transaction
    }

    fn transaction_with_iso(txn_id: shared::TxnId, isolation_level: IsolationLevel) -> Transaction {
        let mut transaction = Transaction::none();
        transaction.isolation_level = isolation_level;
        transaction.txn_id = txn_id;
        transaction
    }
}