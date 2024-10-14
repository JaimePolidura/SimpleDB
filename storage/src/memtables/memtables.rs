use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
use crate::memtables::memtable::MemTable;
use crate::memtables::memtable_iterator::MemtableIterator;
use crate::memtables::wal::Wal;
use crate::transactions::transaction::Transaction;
use bytes::Bytes;
use shared::iterators::merge_iterator::MergeIterator;
use shared::Flag;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use std::sync::{Arc, RwLock};

pub struct Memtables {
    inactive_memtables: AtomicPtr<RwLock<Vec<Arc<MemTable>>>>,
    current_memtable: AtomicPtr<Arc<MemTable>>,
    next_memtable_id: AtomicUsize,
    options: Arc<shared::SimpleDbOptions>,
    keyspace_desc: KeyspaceDescriptor,
}

impl Memtables {
    pub fn create_and_recover_from_wal(
        options: Arc<shared::SimpleDbOptions>,
        keyspace_desc: KeyspaceDescriptor
    ) -> Result<Memtables, shared::SimpleDbError> {
        let (wals, max_memtable_id) = Wal::get_persisted_wal_id(&options, keyspace_desc)?;

        if !wals.is_empty() {
            Self::recover_memtables_from_wal(options, max_memtable_id, wals, keyspace_desc)
        } else {
            Self::create_memtables_no_wal(options, keyspace_desc)
        }
    }

    pub fn scan_all(&self, transaction: &Transaction) -> MergeIterator<MemtableIterator> {
        let iterators = self.create_iterators(transaction);
        MergeIterator::create(iterators)
    }

    pub fn get(&self, key: &Bytes, transaction: &Transaction) -> Option<bytes::Bytes> {
        unsafe {
            let memtable_ref =  (*self.current_memtable.load(Acquire)).clone();
            let value = memtable_ref.get(key, transaction);

            match value {
                Some(value) => Some(value),
                None => self.find_value_in_inactive_memtables(key, transaction),
            }
        }
    }

    pub fn set(&self, key: Bytes, value: &[u8], transaction: &Transaction) -> Option<Arc<MemTable>> {
        unsafe {
            let memtable_ref = (*self.current_memtable.load(Acquire)).clone();
            let set_result = memtable_ref.set(transaction, key, value);

            match set_result {
                Err(_) => self.set_current_memtable_as_inactive(),
                _ => None
            }
        }
    }

    pub fn delete(&self, key: Bytes, transaction: &Transaction) -> Option<Arc<MemTable>> {
        unsafe {
            let memtable_ref = (*self.current_memtable.load(Acquire)).clone();
            let delete_result = memtable_ref.delete(transaction, key);

            match delete_result {
                Err(_) => self.set_current_memtable_as_inactive(),
                _ => None,
            }
        }
    }

    pub fn get_memtable_to_flush(&self, memtable_id: usize) -> Option<Arc<MemTable>> {
        unsafe {
            let current_memtable = (*self.current_memtable.load(Acquire)).clone();
            if current_memtable.get_id() == memtable_id {
                self.set_current_memtable_as_inactive();
            }

            let mut lock_result = self.inactive_memtables.load(Acquire).as_mut()?.write();
            let inactive_memtables = lock_result.as_mut().unwrap();

            match inactive_memtables.iter().position(|item| item.get_id() == memtable_id) {
                Some(inactive_memtable_index) => {
                    Some(inactive_memtables.remove(inactive_memtable_index))
                },
                None => None
            }
        }
    }

    fn create_iterators(&self, transaction: &Transaction) -> Vec<Box<MemtableIterator>> {
        unsafe {
            let mut memtable_iterators: Vec<Box<MemtableIterator>> = Vec::new();

            memtable_iterators.push(Box::from(MemtableIterator::create(
                &(*self.current_memtable.load(Acquire)), transaction, self.keyspace_desc
            )));

            let inactive_memtables_rw_lock = &*self.inactive_memtables.load(Acquire);
            let inactive_memtables_rw_result = inactive_memtables_rw_lock.read().unwrap();

            for memtable in inactive_memtables_rw_result.iter() {
                let cloned = Arc::clone(memtable);
                memtable_iterators.push(Box::new(MemtableIterator::create(&cloned, transaction, self.keyspace_desc)));
            }

            memtable_iterators
        }
    }

    fn find_value_in_inactive_memtables(&self, key: &Bytes, transaction: &Transaction) -> Option<bytes::Bytes> {
        unsafe {
            let inactive_memtables_rw_lock = &*self.inactive_memtables.load(Acquire);
            let inactive_memtables = inactive_memtables_rw_lock.read()
                .unwrap();

            for inactive_memtable in inactive_memtables.iter().rev() {
                if let Some(value) = inactive_memtable.get(key, transaction) {
                    return Some(value);
                }
            }

            None
        }
    }

    //Replaces current_memtable with a new one, and moves old current_memtable to self::inactive_memtables vector
    //Returns a memtable to flush
    //This might be called by concurrently, it might fail returing None
    fn set_current_memtable_as_inactive(&self) -> Option<Arc<MemTable>> {
        let new_memtable_id = self.next_memtable_id.fetch_add(1, Relaxed) as shared::MemtableId;
        let new_memtable = MemTable::create_new(self.options.clone(), new_memtable_id, self.keyspace_desc)
            .expect("Failed to create memtable");
        new_memtable.set_active();
        let new_memtable = Box::into_raw(Box::new(Arc::new(new_memtable)));
        let current_memtable = self.current_memtable.load(Acquire);

        match self.current_memtable.compare_exchange(current_memtable, new_memtable, Release, Relaxed) {
            Ok(prev_memtable) => unsafe { self.move_current_memtable_inactive_list(prev_memtable) },
            Err(_) => { self.next_memtable_id.fetch_sub(1, Relaxed); None }
        }
    }

    //Inserts prev_memtable to self::inactive_memtables vector
    //When the list is full, it returns an option with a memtable to flush
    unsafe fn move_current_memtable_inactive_list(&self, prev_memtable: * mut Arc<MemTable>) -> Option<Arc<MemTable>> {
        let mut memtables_rw_result = self.inactive_memtables.load(Acquire)
            .as_mut()?
            .write();
        let memtables = memtables_rw_result
            .as_mut()
            .unwrap();

        (*prev_memtable).set_inactive();

        memtables.push((*prev_memtable).clone());

        if memtables.len() > self.options.max_memtables_inactive {
            let memtable_to_flush = memtables.remove(0);
            memtable_to_flush.set_flushing();
            return Some(memtable_to_flush);
        }

        None
    }

    fn recover_memtables_from_wal(
        options: Arc<shared::SimpleDbOptions>,
        max_memtable_id: usize,
        wals: Vec<Wal>,
        keyspace_desc: KeyspaceDescriptor
    ) -> Result<Memtables, shared::SimpleDbError> {
        let mut active_memtable = None;
        let mut inactive_memtables: Vec<Arc<MemTable>> = Vec::new();
        let next_memtable_id = max_memtable_id + 1;

        for wal in wals {
            let memtable_id = wal.get_memtable_id();
            let memtable_created = MemTable::create_and_recover_from_wal(
                options.clone(), memtable_id, keyspace_desc, wal
            )?;

            if memtable_created.current_size_bytes.load(Relaxed) < options.memtable_max_size_bytes && active_memtable.is_none() {
                //Active memtable
                memtable_created.set_active();
                active_memtable = Some(memtable_created);
            } else {
                //Inactive memtable
                memtable_created.set_inactive();
                inactive_memtables.push(Arc::new(memtable_created));
            }
        }

        let active_memtable = match active_memtable {
            Some(active_memtable) => active_memtable,
            None => {
                let active_memtable = MemTable::create_new(options.clone(), max_memtable_id + 1, keyspace_desc)?;
                active_memtable.set_active();
                active_memtable
            }
        };

        Ok(Memtables {
            inactive_memtables: AtomicPtr::new(Box::into_raw(Box::new(RwLock::new(inactive_memtables)))),
            current_memtable: AtomicPtr::new(Box::into_raw(Box::new(Arc::new(active_memtable)))),
            next_memtable_id: AtomicUsize::new(next_memtable_id),
            keyspace_desc,
            options,
        })
    }

    fn create_memtables_no_wal(
        options: Arc<shared::SimpleDbOptions>,
        keyspace_desc: KeyspaceDescriptor
    ) -> Result<Memtables, shared::SimpleDbError> {
        let current_memtable = MemTable::create_new(options.clone(), 0, keyspace_desc)?;
        current_memtable.set_active();

        Ok(Memtables {
            inactive_memtables: AtomicPtr::new(Box::into_raw(Box::new(RwLock::new(Vec::with_capacity(options.max_memtables_inactive))))),
            current_memtable: AtomicPtr::new(Box::into_raw(Box::new(Arc::new(current_memtable)))),
            next_memtable_id: AtomicUsize::new(1),
            keyspace_desc,
            options
        })
    }
}