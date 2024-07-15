use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::memtables::memtable::MemTable;
use crate::utils::atomic_shared_ref::AtomicSharedRef;

pub struct Memtables {
    current_memtable: AtomicSharedRef<MemTable>,
    inactive_memtables: Vec<MemTable>,

    options: LsmOptions,
    next_memtable_id: AtomicUsize
}

impl Memtables {
    pub fn new(options: LsmOptions) -> Memtables {
        Memtables {
            current_memtable: AtomicSharedRef::new(MemTable::new(&options, 0)),
            next_memtable_id: AtomicUsize::new(0),
            inactive_memtables: Vec::new(),
            options
        }
    }

    pub fn get(&self, key: &Key) -> Option<bytes::Bytes> {
        let memtable_ref = self.current_memtable.load_ref();
        let value = memtable_ref.shared_ref.get(key);
        self.current_memtable.unload_ref(memtable_ref);

        match value {
            Some(value) => Some(value),
            None => self.find_value_in_inactive_memtables(key),
        }
    }

    pub fn set(&mut self, key: &Key, value: &[u8]) {
        let memtable_ref = self.current_memtable.load_ref();
        let set_result = memtable_ref.shared_ref.set(key, value);
        self.current_memtable.unload_ref(memtable_ref);

        match set_result {
            Err(_) => self.try_flush_memtable(),
            _ => {}
        };
    }

    pub fn delete(&mut self, key: &Key) {
        let memtable_ref = self.current_memtable.load_ref();
        let delete_result = memtable_ref.shared_ref.delete(key);
        self.current_memtable.unload_ref(memtable_ref);

        match delete_result {
            Err(_) => self.try_flush_memtable(),
            _ => {},
        }
    }

    fn find_value_in_inactive_memtables(&self, key: &Key) -> Option<bytes::Bytes> {
        for inactive_memtable in self.inactive_memtables.iter().rev() {
            if let Some(value) = inactive_memtable.get(key) {
                return Some(value);
            }
        }

        None
    }

    fn try_flush_memtable(&mut self) {
        let new_memtable_id = self.next_memtable_id.fetch_add(1, Relaxed);
        let new_memtable = MemTable::new(&self.options, new_memtable_id);

        self.inactive_memtables.push(new_memtable);
    }
}