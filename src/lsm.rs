use std::sync::Arc;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::memtables::memtable::MemtableIterator;
use crate::memtables::memtables::Memtables;
use crate::sst::sstable::SSTable;
use crate::sst::ssttable_iterator::SSTableIterator;
use crate::utils::merge_iterator::MergeIterator;
use crate::utils::two_merge_iterators::TwoMergeIterator;

pub struct Lsm {
    options: LsmOptions,
    memtables: Memtables,
    sstables: Vec<Arc<SSTable>>,
}

impl Lsm {
    pub fn new(lsm_options: LsmOptions) -> Lsm {
        Lsm {
            options: lsm_options,
            memtables: Memtables::new(lsm_options),
            sstables: Vec::new(),
        }
    }

    pub fn scan(&self) -> TwoMergeIterator<MergeIterator<MemtableIterator>, SSTableIterator> {
        TwoMergeIterator::new(
            self.memtables.scan(),
            SSTableIterator::new(self.sstables[0].clone())
        )
    }

    pub fn get(&self, key: &Key) -> Option<bytes::Bytes> {
        match self.memtables.get(key) {
            Some(value_from_memtable) => Some(value_from_memtable),
            None => self.search_in_sstables(key),
        }
    }

    fn search_in_sstables(&self, key: &Key) -> Option<bytes::Bytes> {
        for sstable in &self.sstables {
            match sstable.get(key) {
                Some(value) => return Some(value),
                None => continue
            }
        }

        None
    }

    pub fn set(&mut self, key: &Key, value: &[u8]) {
        self.memtables.set(key, value);
    }

    pub fn delete(&mut self, key: &Key) {
        self.memtables.delete(key);
    }
}