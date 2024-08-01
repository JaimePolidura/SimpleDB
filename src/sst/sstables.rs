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
    sstables: Vec<RwLock<Vec<Arc<SSTable>>>>,
    next_memtable_id: AtomicUsize,
    lsm_options: Arc<LsmOptions>,
    n_current_levels: usize,
    path_buff: PathBuf
}

impl SSTables {
    pub fn new(lsm_options: Arc<LsmOptions>) -> SSTables {
        let mut levels: Vec<RwLock<Vec<Arc<SSTable>>>> = Vec::with_capacity(64);
        for _ in 0..64 {
            levels.push(RwLock::new(Vec::new()));
        }
        SSTables {
            sstables: levels,
            next_memtable_id: AtomicUsize::new(0),
            lsm_options,
            n_current_levels: 0,
            path_buff: PathBuf::new(),
        }
    }

    pub fn scan(&self) -> MergeIterator<SSTableIterator> {
        let mut iterators: Vec<Box<SSTableIterator>> = Vec::with_capacity(self.sstables.len());

        for sstables_in_level_lock in self.sstables.iter() {
            let lock_result = sstables_in_level_lock.read();
            let sstable_in_level = lock_result.as_ref().unwrap();

            for sstable in sstable_in_level.iter() {
                iterators.push(Box::new(SSTableIterator::new(sstable.clone())));
            }
        }

        MergeIterator::new(iterators)
    }

    pub fn get(&self, key: &Key) -> Option<bytes::Bytes> {
        for sstables_in_level_lock in self.sstables.iter() {
            let lock_result = sstables_in_level_lock.read();
            let sstable_in_level = lock_result.as_ref().unwrap();

            for sstable in sstable_in_level.iter() {
                match sstable.get(key) {
                    Some(value) => return Some(value),
                    None => continue
                }
            }
        }

        None
    }

    pub fn delete_sstables(&mut self, level: usize, sstables_id: Vec<usize>) {
        match self.sstables.get(level) {
            Some(sstables_lock) => {
                let mut lock_result = sstables_lock.write();
                let mut sstables_in_level = lock_result.as_mut().unwrap();
                let mut indexes_to_remove = Vec::new();

                for (current_index, current_sstable) in sstables_in_level.iter().enumerate() {
                    for sstable_to_delete in sstables_id.iter() {
                        if *sstable_to_delete == current_sstable.id {
                            indexes_to_remove.push(current_index);
                        }
                    }
                }

                for index_to_remove in indexes_to_remove.iter().rev() {
                    let mut sstable = sstables_in_level.remove(*index_to_remove);
                    sstable.delete();
                }
            },
            None => {}
        }
    }

    pub fn get_sstables(&self, level: usize) -> Vec<Arc<SSTable>> {
        match self.sstables.get(level) {
            Some(sstables) => sstables.read().unwrap().clone(),
            None => Vec::new(),
        }
    }

    pub fn get_sstables_id(&self, level: usize) -> Vec<usize> {
        match self.sstables.get(level) {
            Some(sstables) => sstables.read().unwrap()
                .iter()
                .map(|it| it.level as usize)
                .collect(),
            None => Vec::new(),
        }
    }

    pub fn get_n_sstables(&self, level: usize) -> usize {
        match self.sstables.get(level) {
            Some(sstables_lock) => sstables_lock.read().unwrap().len(),
            None => 0
        }
    }

    pub fn get_n_levels(&self) -> usize {
        self.n_current_levels
    }

    pub fn flush_to_disk(&mut self, sstable_builder: SSTableBuilder) -> Result<usize, ()> {
        let sstable_id: usize = self.next_memtable_id.fetch_add(1, Relaxed);

        //SSTable file path
        self.path_buff = PathBuf::from(self.lsm_options.base_path.to_string());
        self.path_buff.push(sstable_id.to_string());

        let sstable_build_result = sstable_builder.build(
            sstable_id,
            self.path_buff.as_path(),
        );

        match sstable_build_result {
            Ok(sstable_built) => {
                let sstables_in_level_lock = &self.sstables[sstable_built.level as usize];
                let mut lock_result = sstables_in_level_lock.write();
                let sstables_in_level = lock_result.as_mut().unwrap();
                sstables_in_level.push(Arc::new(sstable_built));
                Ok(sstable_id)
            },
            Err(_) => Err(()),
        }
    }
}