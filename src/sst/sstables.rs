use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::sst::sstable::SSTable;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::ssttable_iterator::SSTableIterator;
use crate::utils::merge_iterator::MergeIterator;

pub struct SSTables {
    //For each level one index entry
    sstables: RwLock<Vec<Vec<Arc<SSTable>>>>,
    next_memtable_id: AtomicUsize,
    lsm_options: Arc<LsmOptions>,
    n_current_levels: usize,
}

impl SSTables {
    pub fn new(lsm_options: Arc<LsmOptions>) -> SSTables {
        let mut levels: Vec<Vec<Arc<SSTable>>> = Vec::with_capacity(64);
        for _ in 0..64 {
            levels.push(Vec::new());
        }
        SSTables {
            sstables: RwLock::new(levels),
            next_memtable_id: AtomicUsize::new(0),
            lsm_options,
            n_current_levels: 0,
        }
    }

    pub fn scan(&self) -> MergeIterator<SSTableIterator> {
        let lock_result = self.sstables.read();
        let sstables = lock_result
            .as_ref()
            .unwrap();
        let mut iterators: Vec<Box<SSTableIterator>> = Vec::with_capacity(sstables.len());

        for sstables_in_level in sstables.iter() {
            for sstable in sstables_in_level {
                iterators.push(Box::new(SSTableIterator::new(sstable.clone())));
            }
        }

        MergeIterator::new(iterators)
    }

    pub fn get(&self, key: &Key) -> Option<bytes::Bytes> {
        let lock_result = self.sstables.read();
        let sstables = lock_result
            .as_ref()
            .unwrap();

        for sstables_in_level in sstables.iter() {
            for sstable in sstables_in_level {
                match sstable.get(key) {
                    Some(value) => return Some(value),
                    None => continue
                }
            }
        }

        None
    }

    pub fn get_n_sst_files(&self, level: usize) -> usize {
        match self.sstables.read().unwrap().get(level) {
            Some(sstables) => sstables.len(),
            None => 0
        }
    }

    pub fn get_n_levels(&self) -> usize {
        self.n_current_levels
    }

    pub fn flush_to_disk(&mut self, sstable_builder: SSTableBuilder) -> Result<(), ()> {
        let mut lock_result = self.sstables.write();
        let sstable_id: usize = self.next_memtable_id.fetch_add(1, Relaxed);
        let sstables = lock_result
            .as_mut()
            .unwrap();

        let mut sstable_build_result = sstable_builder.build(
            sstable_id,
            &self.get_path_sstable_flush(sstable_id),
        );

        match sstable_build_result {
            Ok(sstable_built) => {
                sstables[sstable_built.level as usize].push(Arc::new(sstable_built));
                Ok(())
            },
            Err(_) => Err(()),
        }
    }

    fn get_path_sstable_flush(&self, sstable_id: usize) -> &Path {
        let mut path_buff = PathBuf::from(self.lsm_options.base_path.to_string());
        path_buff.push(sstable_id.to_string());
        path_buff.as_path()
    }
}