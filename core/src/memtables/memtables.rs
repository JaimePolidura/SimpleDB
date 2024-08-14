use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use crate::key::Key;
use crate::lsm_error::LsmError;
use crate::lsm_options::LsmOptions;
use crate::memtables::memtable::{MemTable, MemtableIterator};
use crate::memtables::wal::Wal;
use crate::utils::merge_iterator::MergeIterator;

pub struct Memtables {
    current_memtable: AtomicPtr<Arc<MemTable>>,
    inactive_memtables: AtomicPtr<RwLock<Vec<Arc<MemTable>>>>,

    options: Arc<LsmOptions>,
    next_memtable_id: AtomicUsize
}

impl Memtables {
    pub fn create(options: Arc<LsmOptions>) -> Result<Memtables, LsmError> {
        let (wals, max_memtable_id) = Wal::get_persisted_wal_id(&options)?;

        if !wals.is_empty() {
            Self::recover_memtables_from_wal(options, max_memtable_id, wals)
        } else {
            Self::create_memtables_no_wal(options)
        }
    }

    pub fn scan(&self) -> MergeIterator<MemtableIterator> {
        unsafe {
            let mut memtable_iterators: Vec<Box<MemtableIterator>> = Vec::new();

            memtable_iterators.push(Box::from(MemtableIterator::new(&(*self.current_memtable.load(Acquire)))));

            let inactive_memtables_rw_lock = &*self.inactive_memtables.load(Acquire);
            let inactive_memtables_rw_result = inactive_memtables_rw_lock.read().unwrap();

            for memtable in inactive_memtables_rw_result.iter() {
                let cloned = Arc::clone(memtable);
                memtable_iterators.push(Box::new(MemtableIterator::new(&cloned)));
            }

            MergeIterator::new(memtable_iterators)
        }
    }

    pub fn get(&self, key: &Key) -> Option<bytes::Bytes> {
        unsafe {
            let memtable_ref =  (*self.current_memtable.load(Acquire)).clone();
            let value = memtable_ref.get(key);

            match value {
                Some(value) => Some(value),
                None => self.find_value_in_inactive_memtables(key),
            }
        }
    }

    pub fn set(&mut self, key: &Key, value: &[u8]) -> Option<Arc<MemTable>> {
        unsafe {
            let memtable_ref = (*self.current_memtable.load(Acquire)).clone();
            let set_result = memtable_ref.set(key, value);

            match set_result {
                Err(_) => self.set_current_memtable_as_inactive(),
                _ => None
            }
        }
    }

    pub fn delete(&mut self, key: &Key) -> Option<Arc<MemTable>> {
        unsafe {
            let memtable_ref = (*self.current_memtable.load(Acquire)).clone();
            let delete_result = memtable_ref.delete(key);

            match delete_result {
                Err(_) => self.set_current_memtable_as_inactive(),
                _ => None,
            }
        }
    }

    pub fn get_memtable_to_flush(&mut self, memtable_id: usize) -> Option<Arc<MemTable>> {
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

    fn find_value_in_inactive_memtables(&self, key: &Key) -> Option<bytes::Bytes> {
        unsafe {
            let inactive_memtables_rw_lock = &*self.inactive_memtables.load(Acquire);
            let inactive_memtables = inactive_memtables_rw_lock.read()
                .unwrap();

            for inactive_memtable in inactive_memtables.iter().rev() {
                if let Some(value) = inactive_memtable.get(key) {
                    return Some(value);
                }
            }

            None
        }
    }

    //Replaces current_memtable with a new one, and moves old current_memtable to self::inactive_memtables vector
    //Returns a memtable to flush
    //This might be called by concurrently, it might fail returing None
    fn set_current_memtable_as_inactive(&mut self) -> Option<Arc<MemTable>> {
        let new_memtable_id = self.next_memtable_id.fetch_add(1, Relaxed);
        let new_memtable = MemTable::create_new(self.options.clone(), new_memtable_id).expect("Failed to create memtable");
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
    unsafe fn move_current_memtable_inactive_list(&mut self, prev_memtable: * mut Arc<MemTable>) -> Option<Arc<MemTable>> {
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
        options: Arc<LsmOptions>,
        max_memtable_id: usize,
        wals: Vec<Wal>,
    ) -> Result<Memtables, LsmError> {
        let current_memtable = MemTable::create_new(options.clone(), max_memtable_id + 1)?;
        let mut memtables: Vec<Arc<MemTable>> = Vec::new();
        let next_memtable_id = max_memtable_id + 2;

        for wal in wals {
            let memtable_id = wal.get_memtable_id();
            let mut memtable = MemTable::create_and_recover_from_wal(options.clone(), memtable_id, wal)?;
            memtable.set_inactive();
            memtables.push(Arc::new(memtable));
        }

        Ok(Memtables {
            current_memtable: AtomicPtr::new(Box::into_raw(Box::new(Arc::new(current_memtable)))),
            next_memtable_id: AtomicUsize::new(next_memtable_id),
            inactive_memtables: AtomicPtr::new(Box::into_raw(Box::new(RwLock::new(memtables)))),
            options
        })
    }

    fn create_memtables_no_wal(options: Arc<LsmOptions>) -> Result<Memtables, LsmError> {
        let current_memtable = MemTable::create_new(options.clone(), 0)?;
        current_memtable.set_active();

        Ok(Memtables {
            current_memtable: AtomicPtr::new(Box::into_raw(Box::new(Arc::new(current_memtable)))),
            next_memtable_id: AtomicUsize::new(1),
            inactive_memtables: AtomicPtr::new(Box::into_raw(Box::new(RwLock::new(Vec::with_capacity(options.max_memtables_inactive))))),
            options
        })
    }
}