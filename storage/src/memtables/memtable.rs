use std::cell::UnsafeCell;
use std::collections::HashSet;
use std::ops::Bound::{Excluded, Included};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use bytes::Bytes;
use crossbeam_skiplist::{SkipList, SkipMap, SkipSet};
use crate::key;
use crate::key::Key;
use crate::lsm_error::LsmError;
use crate::lsm_options::LsmOptions;
use crate::memtables::memtable::MemtableState::{Active, Flushed, Flusing, Inactive, RecoveringFromWal};
use crate::memtables::wal::Wal;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::transactions::transaction::{Transaction, TxnId};
use crate::transactions::transaction_manager::TransactionManager;
use crate::utils::storage_iterator::{StorageIterator};

const TOMBSTONE: Bytes = Bytes::new();

pub type MemtableId = usize;

pub struct MemTable {
    data: Arc<SkipMap<Key, Bytes>>,
    current_size_bytes: AtomicUsize,
    max_size_bytes: usize,
    memtable_id: MemtableId,
    state: UnsafeCell<MemtableState>,
    wal: UnsafeCell<Wal>,
    options: Arc<LsmOptions>,
    txn_ids_written: SkipSet<TxnId>,
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
        options: Arc<LsmOptions>,
        memtable_id: MemtableId
    ) -> Result<MemTable, LsmError> {
        Ok(MemTable {
            wal: UnsafeCell::new(Wal::create(options.clone(), memtable_id)?),
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::New),
            data: Arc::new(SkipMap::new()),
            txn_ids_written: SkipSet::new(),
            memtable_id,
            options,
        })
    }

    pub fn create_mock(
        options: Arc<LsmOptions>,
        memtable_id: MemtableId
    ) -> Result<MemTable, LsmError> {
        Ok(MemTable {
            wal: UnsafeCell::new(Wal::create_mock(options.clone(), memtable_id)?),
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::Active),
            data: Arc::new(SkipMap::new()),
            txn_ids_written: SkipSet::new(),
            memtable_id,
            options,
        })
    }

    pub fn create_and_recover_from_wal(
        options: Arc<LsmOptions>,
        memtable_id: MemtableId,
        wal: Wal
    ) -> Result<MemTable, LsmError> {
        let mut memtable = MemTable {
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::New),
            data: Arc::new(SkipMap::new()),
            wal: UnsafeCell::new(wal),
            txn_ids_written: SkipSet::new(),
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
        let mut sstable_builder = SSTableBuilder::create(self.options.clone(), 0);
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
            MemtableState::Active | MemtableState::RecoveringFromWal => true,
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

    n_elements_iterated: usize,

    transaction: Transaction,
}

impl MemtableIterator {
    pub fn create(memtable: &Arc<MemTable>, transaction: &Transaction) -> MemtableIterator {
        MemtableIterator {
            transaction: transaction.clone(),
            memtable: memtable.clone(),
            current_value: None,
            n_elements_iterated: 0,
            current_key: None,
        }
    }

    fn move_to_next_key(&mut self) {
        if self.n_elements_iterated == 0 {
            return;
        }

        match &self.current_key {
            Some(last_key) => {
                while let Some(next_entry) = self.memtable.data.lower_bound(Excluded(last_key)) {
                    self.n_elements_iterated = self.n_elements_iterated + 1;

                    if !next_entry.key().as_str().eq(last_key.as_str()) {
                        self.current_value = Some(next_entry.value().clone());
                        self.current_key = Some(next_entry.key().clone());
                        break;
                    }
                }
            },
            None => {}
        };
    }

    fn move_to_most_up_to_date_key(&mut self) -> bool {
        let mut has_advanced = false;

        loop {
            match &self.current_key {
                Some(current_key) => {
                    if let Some(next_entry) = self.memtable.data.lower_bound(Excluded(current_key)) {
                        let next_key = next_entry.key();

                        if current_key.as_str().eq(next_key.as_str()) && self.transaction.can_read(next_key) {
                            self.n_elements_iterated = self.n_elements_iterated + 1;
                            self.current_value = Some(next_entry.value().clone());
                            self.current_key = Some(next_entry.key().clone());
                            has_advanced = true;
                            continue;
                        }
                    }
                }
                None => {
                    self.current_value = Some(self.memtable.data.iter().next().expect("Illegal iterator state").value().clone());
                    self.current_key = Some(self.memtable.data.iter().next().expect("Illegal iterator state").key().clone());
                    self.n_elements_iterated = self.n_elements_iterated + 1;
                    has_advanced = true;
                    continue;
                }
            }

            return has_advanced;
        }
    }
}

impl<'a> StorageIterator for MemtableIterator {
    fn next(&mut self) -> bool {
        if self.memtable.data.is_empty() {
            return false;
        }
        if self.n_elements_iterated > 0 {
            self.move_to_next_key();
        }

        self.move_to_most_up_to_date_key()
    }

    fn has_next(&self) -> bool {
        self.n_elements_iterated < self.memtable.data.len()
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
    use std::sync::Arc;
    use crate::key;
    use crate::lsm_options::LsmOptions;
    use crate::memtables::memtable::{MemTable, MemtableIterator};
    use crate::transactions::transaction::{Transaction, TxnId};
    use crate::utils::storage_iterator::StorageIterator;

    #[test]
    fn get_set_delete() {
        let memtable = MemTable::create_mock(Arc::new(LsmOptions::default()), 0)
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
    fn iterators_readuncommited() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(LsmOptions::default()), 0).unwrap());
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

    fn transaction(txn_id: TxnId) -> Transaction {
        let mut transaction = Transaction::none();
        transaction.txn_id = txn_id;
        transaction
    }
}