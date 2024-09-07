use crate::key;
use crate::key::Key;
use crate::lsm_error::LsmError;
use crate::memtables::memtable::MemtableState::{Active, Flushed, Flusing, Inactive, RecoveringFromWal};
use crate::memtables::wal::Wal;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::transactions::transaction::{Transaction, TxnId};
use crate::transactions::transaction_manager::TransactionManager;
use crate::utils::storage_iterator::StorageIterator;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::{SkipMap, SkipSet};
use std::cell::UnsafeCell;
use std::ops::Bound::Excluded;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use crate::lsm::KeyspaceId;

const TOMBSTONE: Bytes = Bytes::new();

pub type MemtableId = usize;

pub struct MemTable {
    data: Arc<SkipMap<Key, Bytes>>,
    current_size_bytes: AtomicUsize,
    max_size_bytes: usize,
    memtable_id: MemtableId,
    state: UnsafeCell<MemtableState>,
    wal: UnsafeCell<Wal>,
    options: Arc<shared::SimpleDbOptions>,
    txn_ids_written: SkipSet<TxnId>,
    keyspace_id: KeyspaceId,
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
        memtable_id: MemtableId,
        keyspace_id: KeyspaceId
    ) -> Result<MemTable, LsmError> {
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
        memtable_id: MemtableId
    ) -> Result<MemTable, LsmError> {
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
        memtable_id: MemtableId,
        keyspace_id: KeyspaceId,
        wal: Wal
    ) -> Result<MemTable, LsmError> {
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

    pub fn has_txn_id_been_written(&self, txn_id: TxnId) -> bool {
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

    pub fn get_id(&self) -> MemtableId {
        self.memtable_id
    }

    pub fn get(&self, key_lookup: &str, transaction: &Transaction) -> Option<Bytes> {
        let mut current_key = key::new(key_lookup, transaction.txn_id + 1);

        loop {
            if let Some(entry) = self.data.upper_bound(Excluded(&current_key)) {
                if !entry.key().as_str().eq(key_lookup) {
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

    pub fn set(&self, transaction: &Transaction, key: &str, value: &[u8]) -> Result<(), LsmError> {
        self.write_into_skip_list(
            &key::new(key, transaction.txn_id),
            Bytes::copy_from_slice(value),
            transaction.txn_id
        )
    }

    pub fn delete(&self, transaction: &Transaction, key: &str) -> Result<(), LsmError> {
        self.write_into_skip_list(
            &key::new(key, transaction.txn_id),
            TOMBSTONE,
            transaction.txn_id
        )
    }

    fn write_into_skip_list(&self, key: &Key, value: Bytes, txn_id: TxnId) -> Result<(), LsmError> {
        if !self.can_memtable_be_written() {
            return Ok(());
        }
        if self.current_size_bytes.load(Relaxed) >= self.max_size_bytes {
            return Err(LsmError::Internal);
        }

        self.write_wal(&key, &value)?;

        self.txn_ids_written.insert(txn_id);

        self.current_size_bytes.fetch_add(key.len() + value.len(), Relaxed);

        self.data.insert(
            key.clone(), value
        );

        Ok(())
    }

    fn write_wal(&self, key: &Key, value: &Bytes) -> Result<(), LsmError> {
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

    fn recover_from_wal(&mut self) -> Result<(), LsmError> {
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

    //Sets current_key & current_value to the next key & value to be returned by the iterator
    //Returns true if it found a key to be returned by the iterator
    fn go_to_next_readble_key(&mut self) -> bool {
        loop {
            match self.move_to_different_key_string(&self.current_key) {
                Some((next_key, next_value)) => {
                    self.current_key = Some(next_key.clone());
                    self.current_value = Some(next_value.clone());

                    match self.move_to_most_up_to_date_key_string(&next_key, &next_value) {
                        Some((new_key, new_value)) => {
                            self.current_value = Some(new_value);
                            self.current_key = Some(new_key);
                            return true;
                        },
                        None => continue
                    }
                },
                None => { return false; }
            };
        }
    }

    //Moves current_key to the next key with different string
    //Returns Some if the key was found
    //Returns None if no key was found to be the next one (reached the end)
    fn move_to_different_key_string(&self, current_key: &Option<Key>) -> Option<(Key, Bytes)> {
        match &current_key {
            Some(last_key) => {
                let mut last_key = last_key.clone();

                while let Some(next_entry) = self.memtable.data.lower_bound(Excluded(&last_key)) {
                    let next_key = next_entry.key().clone();

                    if !next_key.as_str().eq(last_key.as_str()) {
                        return Some((next_key, next_entry.value().clone()));
                    } else {
                        last_key = next_key;
                    }
                }

                None
            },
            None => self.memtable.data.iter().next().map(|entry| (entry.key().clone(), entry.value().clone()))
        }
    }

    //Given a key "start_key", this function will set current_key to the key with the same string as "start_key" which
    //is the most up-to-date readable by the current transaction
    fn move_to_most_up_to_date_key_string(&self, start_key: &Key, start_value: &Bytes) -> Option<(Key, Bytes)> {
        let mut current_key = start_key.clone();
        let mut n_iterations_with_same_string_key = 0;
        let mut has_advanced = false;
        let mut selected_value: Option<Bytes> = None;
        let mut selected_key: Option<Key> = None;


        while let Some(next_entry) = self.memtable.data.lower_bound(Excluded(&current_key)) {
            let next_key = next_entry.key();
            if !current_key.as_str().eq(next_key.as_str()) {
                break;
            }

            n_iterations_with_same_string_key = n_iterations_with_same_string_key + 1;
            current_key = next_entry.key().clone();

            if self.transaction.can_read(next_key) {
                selected_value = Some(next_entry.value().clone());
                selected_key = Some(next_entry.key().clone());
                has_advanced = true;
            }
        }

        if !has_advanced && self.transaction.can_read(start_key) {
            return Some((start_key.clone(), start_value.clone()))
        }

        if has_advanced {
            Some((selected_key.take().unwrap(), selected_value.take().unwrap()))
        } else {
            None
        }
    }
}

impl<'a> StorageIterator for MemtableIterator {
    fn next(&mut self) -> bool {
        if self.memtable.data.is_empty() {
            return false;
        }

        self.go_to_next_readble_key()
    }

    fn has_next(&self) -> bool {
        if self.memtable.data.is_empty() {
            return false;
        }

        let mut current_key = match &self.current_key {
            Some(key) => key.clone(),
            None => self.memtable.data.iter().next().unwrap().key().clone()
        };

        loop {
            match self.move_to_different_key_string(&Some(current_key)) {
                Some((next_key, next_value)) => {
                    match self.move_to_most_up_to_date_key_string(&next_key, &next_value) {
                        Some(_) => return true,
                        None => current_key = next_key,
                    };
                }
                None => return false,
            }
        }
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
    use crate::transactions::transaction::{Transaction, TxnId};
    use crate::transactions::transaction_manager::IsolationLevel;
    use crate::utils::storage_iterator::StorageIterator;
    use std::sync::Arc;

    #[test]
    fn get_set_delete_no_transactions() {
        let memtable = MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0)
            .unwrap();
        let value: Vec<u8> = vec![10, 12];

        assert!(memtable.get("nombre", &Transaction::none()).is_none());

        memtable.set(&Transaction::none(), "nombre", &value);
        memtable.set(&Transaction::none(), "edad", &value);

        assert!(memtable.get("nombre", &Transaction::none()).is_some());
        assert!(memtable.get("edad", &Transaction::none()).is_some());

        memtable.delete(&Transaction::none(), "nombre");

        assert!(memtable.get("nombre", &Transaction::none()).is_some());
    }

    #[test]
    fn get_set_delete_transactions() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0).unwrap());
        memtable.set_active();
        memtable.set(&transaction(10), "aa", &vec![1]); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), "alberto", &vec![2]);
        memtable.set(&transaction(2), "alberto", &vec![3]);
        memtable.set(&transaction(3), "alberto", &vec![4]);
        memtable.set(&transaction(1), "gonchi", &vec![5]);
        memtable.set(&transaction(5), "javier", &vec![6]); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), "jaime", &vec![7]);
        memtable.set(&transaction(5), "jaime", &vec![8]);
        memtable.set(&transaction(0), "wili", &vec![9]);

        let to_test = memtable.get("alberto", &transaction(2));
        assert!(to_test.is_some());
        assert!(to_test.unwrap().eq(&vec![3]));

        let to_test = memtable.get("aa", &transaction(9));
        assert!(to_test.is_none());

        let to_test = memtable.get("jaime", &transaction(6));
        assert!(to_test.is_some());
        assert!(to_test.unwrap().eq(&vec![8]));
    }

    #[test]
    fn iterators_readuncommited() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0).unwrap());
        memtable.set_active();
        let value: Vec<u8> = vec![10, 12];
        memtable.set(&transaction(1), "alberto", &value);
        memtable.set(&transaction(2), "alberto", &value);
        memtable.set(&transaction(3), "alberto", &value);
        memtable.set(&transaction(1), "jaime", &value);
        memtable.set(&transaction(5), "jaime", &value);
        memtable.set(&transaction(1), "gonchi", &value);
        memtable.set(&transaction(0), "wili", &value);

        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());

        assert!(iterator.has_next());
        iterator.next();

        assert!(iterator.key().eq(&key::new("alberto", 3)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::new("gonchi", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::new("jaime", 5)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::new("wili", 0)));

        assert!(!iterator.has_next());
    }

    #[test]
    fn iterators_snapshotisolation() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0).unwrap());
        memtable.set_active();
        let value: Vec<u8> = vec![10, 12];
        memtable.set(&transaction(10), "aa", &value); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), "alberto", &value);
        memtable.set(&transaction(2), "alberto", &value);
        memtable.set(&transaction(3), "alberto", &value);
        memtable.set(&transaction(1), "gonchi", &value);
        memtable.set(&transaction(5), "javier", &value); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), "jaime", &value);
        memtable.set(&transaction(5), "jaime", &value);
        memtable.set(&transaction(0), "wili", &value);

        let mut iterator = MemtableIterator::create(&memtable, &transaction_with_iso(3, IsolationLevel::SnapshotIsolation));

        assert!(iterator.has_next());
        iterator.next();

        assert!(iterator.key().eq(&key::new("alberto", 3)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::new("gonchi", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::new("jaime", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::new("wili", 0)));
    }

    fn transaction(txn_id: TxnId) -> Transaction {
        let mut transaction = Transaction::none();
        transaction.txn_id = txn_id;
        transaction
    }

    fn transaction_with_iso(txn_id: TxnId, isolation_level: IsolationLevel) -> Transaction {
        let mut transaction = Transaction::none();
        transaction.isolation_level = isolation_level;
        transaction.txn_id = txn_id;
        transaction
    }
}