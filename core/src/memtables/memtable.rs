use std::cell::UnsafeCell;
use std::ops::Bound::Excluded;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crate::key::Key;
use crate::lsm_error::LsmError;
use crate::lsm_options::LsmOptions;
use crate::memtables::memtable::MemtableState::{Active, Flushed, Flusing, Inactive, RecoveringFromWal};
use crate::memtables::wal::Wal;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::utils::storage_iterator::{StorageIterator};

const TOMBSTONE: Bytes = Bytes::new();

pub struct MemTable {
    data: Arc<SkipMap<Key, Bytes>>,
    current_size_bytes: AtomicUsize,
    max_size_bytes: usize,
    memtable_id: usize,
    state: UnsafeCell<MemtableState>,
    wal: UnsafeCell<Wal>,
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
        memtable_id: usize
    ) -> Result<MemTable, LsmError> {
        Ok(MemTable {
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::New),
            data: Arc::new(SkipMap::new()),
            wal: UnsafeCell::new(Wal::create(options, memtable_id)?),
            memtable_id
        })
    }

    pub fn create_and_recover_from_wal(
        options: Arc<LsmOptions>,
        memtable_id: usize,
        wal: Wal
    ) -> Result<MemTable, LsmError> {
        let mut memtable = MemTable {
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            state: UnsafeCell::new(MemtableState::New),
            data: Arc::new(SkipMap::new()),
            wal: UnsafeCell::new(wal),
            memtable_id
        };

        memtable.recover_from_wal();
        Ok(memtable)
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

    pub fn get_id(&self) -> usize {
        self.memtable_id
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

    pub fn set(&self, key: &Key, value: &[u8]) -> Result<(), LsmError> {
        self.write_into_skip_list(
            key,
            Bytes::copy_from_slice(value)
        )
    }

    pub fn delete(&self, key: &Key) -> Result<(), LsmError> {
        self.write_into_skip_list(
            key,
            TOMBSTONE
        )
    }

    fn write_into_skip_list(&self, key: &Key, value: Bytes) -> Result<(), LsmError> {
        if !self.can_memtable_be_written() {
            return Ok(());
        }
        if self.current_size_bytes.load(Relaxed) >= self.max_size_bytes {
            return Err(LsmError::Internal);
        }

        self.write_wal(&key, &value)?;

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
            wal.write(key, value)
        } else {
            Ok(())
        }
    }

    pub fn to_sst(options: Arc<LsmOptions>, memtable: Arc<MemTable>) -> SSTableBuilder {
        let mut memtable_iterator = MemtableIterator::new(&memtable);
        let mut sstable_builder = SSTableBuilder::new(options, 0);
        sstable_builder.set_memtable_id(memtable.memtable_id);

        while memtable_iterator.next() {
            let value = memtable_iterator.value();
            let key = memtable_iterator.key();
            sstable_builder.add_entry(key.clone(), Bytes::copy_from_slice(value));
        }

        sstable_builder
    }

    fn recover_from_wal(&mut self) -> Result<(), LsmError> {
        self.set_recovering_from_wal();
        let wal: &Wal = unsafe { &*self.wal.get() };
        let mut entries = wal.read_entries()?;

        println!("Applying {} operations from WAL to memtable with ID: {}", entries.len(), wal.get_memtable_id());

        while let Some(entry) = entries.pop() {
            self.write_into_skip_list(&entry.key, entry.value);
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
}

impl MemtableIterator {
    pub fn new(memtable: &Arc<MemTable>) -> MemtableIterator {
        MemtableIterator {
            memtable: memtable.clone(),
            current_value: None,
            n_elements_iterated: 0,
            current_key: None
        }
    }
}

impl<'a> StorageIterator for MemtableIterator {
    fn next(&mut self) -> bool {
        let mut has_advanced = false;
        if self.memtable.data.is_empty() {
            return has_advanced;
        }

        match &self.current_key {
            Some(prev_key) => {
                if let Some(next_entry) = self.memtable.data.lower_bound(Excluded(prev_key)) {
                    self.n_elements_iterated = self.n_elements_iterated + 1;
                    self.current_value = Some(next_entry.value().clone());
                    self.current_key = Some(next_entry.key().clone());
                    has_advanced = true;
                }
            },
            None => {
                self.current_value = Some(self.memtable.data.iter().next().expect("Illegal iterator state").value().clone());
                self.current_key = Some(self.memtable.data.iter().next().expect("Illegal iterator state").key().clone());
                self.n_elements_iterated = self.n_elements_iterated + 1;
                has_advanced = true;
            }
        }

        has_advanced
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
    use crate::utils::storage_iterator::StorageIterator;

    #[test]
    fn get_set_delete() {
        let memtable = MemTable::create_new(&LsmOptions::default(), 0)
            .unwrap();
        let value: Vec<u8> = vec![10, 12];

        assert!(memtable.get(&key::new("nombre")).is_none());

        memtable.set(&key::new("nombre"), &value);
        memtable.set(&key::new("edad"), &value);

        assert!(memtable.get(&key::new("nombre")).is_some());
        assert!(memtable.get(&key::new("edad")).is_some());

        memtable.delete(&key::new("nombre"));

        assert!(memtable.get(&key::new("nombre")).is_none());
    }

    #[test]
    fn iterators() {
        let memtable = Arc::new(MemTable::create_new(&LsmOptions::default(), 0).unwrap());
        let value: Vec<u8> = vec![10, 12];
        memtable.set(&key::new("alberto"), &value);
        memtable.set(&key::new("jaime"), &value);
        memtable.set(&key::new("gonchi"), &value);
        memtable.set(&key::new("wili"), &value);

        let mut iterator = MemtableIterator::new(&memtable);

        assert!(iterator.has_next());
        iterator.next();

        assert!(iterator.key().eq(&key::new("alberto")));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::new("gonchi")));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::new("jaime")));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::new("wili")));

        assert!(!iterator.has_next());
    }
}