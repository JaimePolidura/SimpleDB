use std::sync::Arc;
use bytes::Bytes;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::memtables::memtable::{MemTable, MemtableIterator};
use crate::memtables::memtables::Memtables;
use crate::sst::sstable::SSTable;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::sst::ssttable_iterator::SSTableIterator;
use crate::utils::merge_iterator::MergeIterator;
use crate::utils::storage_iterator::StorageIterator;
use crate::utils::two_merge_iterators::TwoMergeIterator;

pub struct Lsm {
    options: LsmOptions,
    memtables: Memtables,
    sstables: SSTables,
}

impl Lsm {
    pub fn new(lsm_options: LsmOptions) -> Lsm {
        Lsm {
            options: lsm_options,
            memtables: Memtables::new(lsm_options),
            sstables: SSTables::new(),
        }
    }

    pub fn scan(&self) -> TwoMergeIterator<MergeIterator<MemtableIterator>, MergeIterator<SSTableIterator>> {
        TwoMergeIterator::new(
            self.memtables.scan(),
            self.sstables.scan(),
        )
    }

    pub fn get(&self, key: &Key) -> Option<bytes::Bytes> {
        match self.memtables.get(key) {
            Some(value_from_memtable) => Some(value_from_memtable),
            None => self.sstables.get(key),
        }
    }

    pub fn set(&mut self, key: &Key, value: &[u8]) -> Result<(), ()> {
        match self.memtables.set(key, value) {
            Some(memtable_to_flush) => self.flush_memtable(memtable_to_flush),
            None => Ok(())
        }
    }

    pub fn delete(&mut self, key: &Key) -> Result<(), ()> {
        match self.memtables.delete(key) {
            Some(memtable_to_flush) => self.flush_memtable(memtable_to_flush),
            None => Ok(()),
        }
    }

    fn flush_memtable(&mut self, memtable: Arc<MemTable>) -> Result<(), ()> {
        let sstable_builder_ready: SSTableBuilder = MemTable::to_sst(self.options, memtable);
        self.sstables.flush_to_disk(sstable_builder_ready)
    }
}