use std::sync::{Arc, RwLock};
use std::sync::atomic::{AtomicPtr, AtomicUsize};
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::memtables::memtable::{MemTable, MemtableIterator};
use crate::utils::merge_iterator::MergeIterator;

pub struct Memtables {
    current_memtable: AtomicPtr<Arc<MemTable>>,
    inactive_memtables: AtomicPtr<RwLock<Vec<Arc<MemTable>>>>,

    options: Arc<LsmOptions>,
    next_memtable_id: AtomicUsize
}

impl Memtables {
    pub fn new(options: Arc<LsmOptions>) -> Memtables {
        Memtables {
            current_memtable: AtomicPtr::new(Box::into_raw(Box::new(Arc::new(MemTable::new(&options, 0))))),
            next_memtable_id: AtomicUsize::new(0),
            inactive_memtables: AtomicPtr::new(Box::into_raw(Box::new(RwLock::new(Vec::with_capacity(options.max_memtables_inactive))))),
            options
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
        let new_memtable = Box::into_raw(Box::new(Arc::new(MemTable::new(&self.options, new_memtable_id))));
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

        memtables.push((*prev_memtable).clone());

        if memtables.len() > self.options.max_memtables_inactive {
            return Some(memtables.pop()?);
        }

        None
    }
}