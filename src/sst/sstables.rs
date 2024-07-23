use std::f32::consts::E;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use crate::key::Key;
use crate::sst::sstable::SSTable;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::ssttable_iterator::SSTableIterator;
use crate::utils::merge_iterator::MergeIterator;

pub struct SSTables {
    sstables: RwLock<Vec<Arc<SSTable>>>,
    next_memtable_id: AtomicUsize,
}

impl SSTables {
    pub fn new() -> SSTables {
        SSTables {
            sstables: RwLock::new(Vec::new()),
            next_memtable_id: AtomicUsize::new(0),
        }
    }

    pub fn scan(&self) -> MergeIterator<SSTableIterator> {
        let lock_result = self.sstables.read();
        let sstables = lock_result
            .as_ref()
            .unwrap();
        let mut iterators: Vec<Box<SSTableIterator>> = Vec::with_capacity(sstables.len());

        for sstable in sstables.iter() {
            iterators.push(Box::new(SSTableIterator::new(sstable.clone())));
        }

        MergeIterator::new(iterators)
    }

    pub fn get(&self, key: &Key) -> Option<bytes::Bytes> {
        let lock_result = self.sstables.read();
        let sstables = lock_result
            .as_ref()
            .unwrap();

        for sstable in sstables.iter() {
            match sstable.get(key) {
                Some(value) => return Some(value),
                None => continue
            }
        }

        None
    }

    pub fn flush_to_disk(&mut self, sstable_builder: SSTableBuilder) -> Result<(), ()> {
        let mut lock_result = self.sstables.write();
        let sstables = lock_result
            .as_mut()
            .unwrap();

        let mut sstable_build_result = sstable_builder.build(
            self.next_memtable_id.fetch_add(1, Relaxed),
            &self.get_path_sstable_flush(),
        );

        match sstable_build_result {
            Ok(sstable_built) => {
                sstables.push(Arc::new(sstable_built));
                Ok(())
            },
            Err(_) => Err(()),
        }
    }
    
    fn get_path_sstable_flush(&self) -> &Path {
        unimplemented!();
    }
}