use crate::memtables::memtable::MemtableState::{Active, Flushed, Flushing, Inactive, RecoveringFromWal};
use crate::memtables::memtable_iterator::MemtableIterator;
use crate::memtables::wal::Wal;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::transactions::transaction::Transaction;
use crate::transactions::transaction_manager::TransactionManager;
use shared::iterators::storage_iterator::StorageIterator;
use crate::utils::tombstone::TOMBSTONE;
use bytes::{Buf, Bytes};
use crossbeam_skiplist::{SkipMap, SkipSet};
use shared::{Flag, StorageValueMergeResult};
use std::cell::UnsafeCell;
use std::ops::Bound::Excluded;
use std::ops::Shl;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use shared::key::Key;

pub struct MemTable {
    pub(crate) data: Arc<SkipMap<Key, Bytes>>,
    pub(crate) current_size_bytes: AtomicUsize,
    pub(crate) max_size_bytes: usize,
    pub(crate) memtable_id: shared::MemtableId,
    pub(crate) state: UnsafeCell<MemtableState>,
    pub(crate) wal: UnsafeCell<Wal>,
    pub(crate) options: Arc<shared::SimpleDbOptions>,
    pub(crate) txn_ids_written: SkipSet<shared::TxnId>,
    pub(crate) keyspace_id: shared::KeyspaceId,
    pub(crate) keyspace_flags: Flag,
}

enum MemtableState {
    New,
    RecoveringFromWal,
    Active,
    Inactive,
    Flushing,
    Flushed
}

impl MemTable {
    pub fn create_new(
        options: Arc<shared::SimpleDbOptions>,
        memtable_id: shared::MemtableId,
        keyspace_id: shared::KeyspaceId,
        keyspace_flags: Flag
    ) -> Result<MemTable, shared::SimpleDbError> {
        Ok(MemTable {
            wal: UnsafeCell::new(Wal::create(options.clone(), keyspace_id, memtable_id)?),
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::New),
            data: Arc::new(SkipMap::new()),
            txn_ids_written: SkipSet::new(),
            keyspace_flags,
            keyspace_id,
            memtable_id,
            options,
        })
    }

    pub fn create_mock(
        options: Arc<shared::SimpleDbOptions>,
        memtable_id: shared::MemtableId,
        keyspace_flags: Flag
    ) -> Result<MemTable, shared::SimpleDbError> {
        Ok(MemTable {
            wal: UnsafeCell::new(Wal::create_mock(options.clone(), memtable_id)?),
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::Active),
            data: Arc::new(SkipMap::new()),
            txn_ids_written: SkipSet::new(),
            keyspace_id: 0,
            keyspace_flags,
            memtable_id,
            options,
        })
    }

    pub fn create_and_recover_from_wal(
        options: Arc<shared::SimpleDbOptions>,
        memtable_id: shared::MemtableId,
        keyspace_id: shared::KeyspaceId,
        keyspace_flags: Flag,
        wal: Wal
    ) -> Result<MemTable, shared::SimpleDbError> {
        let mut memtable = MemTable {
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::New),
            data: Arc::new(SkipMap::new()),
            wal: UnsafeCell::new(wal),
            txn_ids_written: SkipSet::new(),
            keyspace_flags,
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
        unsafe { (* self.state.get()) = Flushing; }
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
        let mut current_key = Key::create(key_lookup.clone(), transaction.txn_id + 1);

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
        self.write(
            &Key::create(key, transaction.txn_id),
            Bytes::copy_from_slice(value),
            transaction.txn_id
        )
    }

    pub fn delete(&self, transaction: &Transaction, key: Bytes) -> Result<(), shared::SimpleDbError> {
        self.write(
            &Key::create(key, transaction.txn_id),
            TOMBSTONE,
            transaction.txn_id
        )
    }

    fn write(&self, key: &Key, value: Bytes, txn_id: shared::TxnId) -> Result<(), shared::SimpleDbError> {
        if !self.can_memtable_be_written() {
            return Ok(());
        }
        if self.current_size_bytes.load(Relaxed) >= self.max_size_bytes {
            return Err(shared::SimpleDbError::Internal);
        }

        self.write_wal(&key, &value)?;

        self.txn_ids_written.insert(txn_id);

        self.current_size_bytes.fetch_add(key.len() + value.len(), Relaxed);

        self.write_into_skiplist(key, value);

        Ok(())
    }

    //This function will merge the values in the skiplist if they have the same key (key bytes and txn_id)
    //There won't be race conditions if the writes made by one transaction are done sequentially (AKA one after each other).
    //We will only merge keys with the same key bytes & transaction ID, so we will always merge writes made by one transaction
    //to one key
    fn write_into_skiplist(&self, key: &Key, value: Bytes) {
        if self.options.storage_value_merger.is_none() {
            self.data.insert(key.clone(), value);
            return;
        }

        match self.data.get(key) {
            Some(present_entry) => {
                let merger_fn = self.options.storage_value_merger.unwrap();

                match merger_fn(present_entry.value(), &value, self.keyspace_flags) {
                    StorageValueMergeResult::Ok(merged_value) => { self.data.insert(key.clone(), merged_value); }
                    StorageValueMergeResult::DiscardPreviousKeepNew => { self.data.insert(key.clone(), value); }
                    StorageValueMergeResult::DiscardPreviousAndNew => {}
                };
            }
            None => { self.data.insert(key.clone(), value); },
        };
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
            self.write(&entry.key, entry.value, entry.key.txn_id());
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

#[cfg(test)]
mod test {
    use crate::memtables::memtable::MemTable;
    use crate::transactions::transaction::Transaction;
    use crate::transactions::transaction_manager::IsolationLevel;
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    fn get_set_delete_no_transactions() {
        let memtable = MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0, 0)
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
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0, 0).unwrap());
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