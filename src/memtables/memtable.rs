use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use bytes::Bytes;
use crossbeam_skiplist::map::{Entry, Iter};
use crossbeam_skiplist::SkipMap;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::utils::storage_iterator::{StorageIterator};

pub struct MemTable {
    data: Arc<SkipMap<Key, Bytes>>,
    current_size_bytes: AtomicUsize,
    max_size_bytes: usize,
    id: usize,
}

impl MemTable {
    pub fn new(options: &LsmOptions, id: usize) -> MemTable {
        MemTable {
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            data: Arc::new(SkipMap::new()),
            id
        }
    }

    pub fn get(&self, key: &Key) -> Option<&Bytes> {
        // TODO
        // self.data.get(key)
        //     .map(|r| { r.value()})
        None
    }

    pub fn set(&self, key: Key, value: &[u8]) -> Result<(), ()> {
        self.write_into_skip_list(
            key,
            Bytes::copy_from_slice(value)
        )
    }

    pub fn delete(&self, key: Key) -> Result<(), ()> {
        self.write_into_skip_list(
            key,
            Bytes::new()
        )
    }

    fn write_into_skip_list(&self, key: Key, value: Bytes) -> Result<(), ()> {
        if self.current_size_bytes.load(Relaxed) >= self.max_size_bytes {
            return Err(());
        }

        self.current_size_bytes.fetch_add(key.len() + value.len(), Relaxed);

        self.data.insert(
            key, value
        );

        Ok(())
    }

    fn iterator(&self) -> MemtableIterator {
        MemtableIterator::new(self)
    }
}

pub struct MemtableIterator<'a> {
    iterator: Iter<'a, Key, Bytes>,
    current_data: Option<Entry<'a, Key, Bytes>>
}

impl<'a> MemtableIterator<'a> {
    pub fn new(memtable: &'a MemTable) -> MemtableIterator<'a> {
        let mut iterator= memtable.data.iter();

        MemtableIterator {
            current_data: iterator.next(),
            iterator,
        }
    }
}

impl<'a> StorageIterator for MemtableIterator<'a> {
    fn next(&mut self) -> bool {
        self.current_data = self.iterator.next();
        self.has_next()
    }

    fn has_next(&self) -> bool {
        self.current_data.is_some()
    }

    fn key(&self) -> &Key {
        let entry = self.current_data
            .as_ref()
            .expect("Illegal iterator state");

        entry.key()
    }

    fn value(&self) -> &[u8] {
        let entry = self.current_data
            .as_ref()
            .expect("Illegal iterator state");

        entry.value()
    }
}