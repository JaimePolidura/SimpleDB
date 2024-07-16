use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use bytes::Bytes;
use crossbeam_skiplist::map::{Entry, Iter};
use crossbeam_skiplist::SkipMap;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::utils::storage_iterator::{StorageIterator};

const TOMBSTONE: Bytes = Bytes::new();

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

    pub fn get(&self, key: &Key) -> Option<Bytes> {
        match self.data.get(key) {
            Some(entry) => {
                if entry.value() == &TOMBSTONE {
                    return None;
                }

                Some(entry.value().clone())
            }
            None => None,
        }
    }

    pub fn set(&self, key: &Key, value: &[u8]) -> Result<(), ()> {
        self.write_into_skip_list(
            key,
            Bytes::copy_from_slice(value)
        )
    }

    pub fn delete(&self, key: &Key) -> Result<(), ()> {
        self.write_into_skip_list(
            key,
            TOMBSTONE
        )
    }

    pub fn scan(&self) -> MemtableIterator {
        MemtableIterator::new(&self)
    }

    fn write_into_skip_list(&self, key: &Key, value: Bytes) -> Result<(), ()> {
        if self.current_size_bytes.load(Relaxed) >= self.max_size_bytes {
            return Err(());
        }

        self.current_size_bytes.fetch_add(key.len() + value.len(), Relaxed);

        self.data.insert(
            key.clone(), value
        );

        Ok(())
    }

    fn iterator(&self) -> MemtableIterator {
        MemtableIterator::new(self)
    }
}

pub struct MemtableIterator<'a> {
    iterator: Iter<'a, Key, Bytes>,
    memtable: &'a MemTable,

    current_data: Option<Entry<'a, Key, Bytes>>,
    n_elements_iterated: usize,
}

impl<'a> MemtableIterator<'a> {
    pub fn new(memtable: &'a MemTable) -> MemtableIterator<'a> {
        let mut iterator= memtable.data.iter();

        MemtableIterator {
            n_elements_iterated: 0,
            current_data: None,
            memtable,
            iterator,
        }
    }
}

impl<'a> StorageIterator for MemtableIterator<'a> {
    fn next(&mut self) -> bool {
        let next_data = self.iterator.next();
        let has_advanced = next_data.is_some();

        if has_advanced {
            self.n_elements_iterated = self.n_elements_iterated + 1;
            self.current_data = next_data;
        }

        has_advanced
    }

    fn has_next(&self) -> bool {
        self.n_elements_iterated < self.memtable.data.len()
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

#[cfg(test)]
mod test {
    use crate::key::Key;
    use crate::lsm_options::LsmOptions;
    use crate::memtables::memtable::MemTable;
    use crate::utils::storage_iterator::StorageIterator;

    #[test]
    fn get_set_delete() {
        let memtable: MemTable = MemTable::new(&LsmOptions{memtable_max_size_bytes: 1000}, 0);
        let value: Vec<u8> = vec![10, 12];

        assert!(memtable.get(&Key::new("nombre")).is_none());

        memtable.set(&Key::new("nombre"), &value);
        memtable.set(&Key::new("edad"), &value);

        assert!(memtable.get(&Key::new("nombre")).is_some());
        assert!(memtable.get(&Key::new("edad")).is_some());

        memtable.delete(&Key::new("nombre"));

        assert!(memtable.get(&Key::new("nombre")).is_none());
    }

    #[test]
    fn iterators() {
        let memtable: MemTable = MemTable::new(&LsmOptions{memtable_max_size_bytes: 1000}, 0);
        let value: Vec<u8> = vec![10, 12];
        memtable.set(&Key::new("alberto"), &value);
        memtable.set(&Key::new("jaime"), &value);
        memtable.set(&Key::new("gonchi"), &value);
        memtable.set(&Key::new("wili"), &value);

        let mut iterator = memtable.scan();

        assert!(iterator.has_next());
        iterator.next();

        assert!(iterator.key().eq(&Key::new("alberto")));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&Key::new("gonchi")));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&Key::new("jaime")));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&Key::new("wili")));

        assert!(!iterator.has_next());
    }
}