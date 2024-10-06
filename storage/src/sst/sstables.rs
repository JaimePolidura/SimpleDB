use std::cmp::max;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables_files::{extract_sstable_id_from_file, is_sstable_file, to_sstable_file_name};
use crate::sst::ssttable_iterator::SSTableIterator;
use shared::iterators::merge_iterator::MergeIterator;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::sync::{Arc, RwLock};
use bytes::Bytes;
use shared::iterators::storage_iterator::StorageIterator;
use crate::manifest::manifest::{Manifest, ManifestOperationContent, MemtableFlushManifestOperation};
use crate::sst::sstable::{SSTable, SSTABLE_ACTIVE};
use crate::transactions::transaction::Transaction;

pub struct SSTables {
    //For each level one index entry
    sstables: Vec<RwLock<Vec<Arc<SSTable>>>>,
    keyspace_id: shared::KeyspaceId,
    next_sstable_id: AtomicUsize,
    options: Arc<shared::SimpleDbOptions>,
    manifest: Arc<Manifest>,
    n_current_levels: usize,
}

impl SSTables {
    pub fn open(
        options: Arc<shared::SimpleDbOptions>,
        keyspace_id: shared::KeyspaceId,
        manifest: Arc<Manifest>
    ) -> Result<SSTables, shared::SimpleDbError> {
        let mut levels: Vec<RwLock<Vec<Arc<SSTable>>>> = Vec::with_capacity(64);
        for _ in 0..64 {
            levels.push(RwLock::new(Vec::new()));
        }
        let (sstables, max_ssatble_id) = Self::load_sstables(&options, keyspace_id)?;

        Ok(SSTables {
            keyspace_id,
            next_sstable_id: AtomicUsize::new(max_ssatble_id + 1),
            n_current_levels: 0,
            options,
            sstables,
            manifest,
        })
    }

    fn load_sstables(
        options: &Arc<shared::SimpleDbOptions>,
        keyspace_id: shared::KeyspaceId
    ) -> Result<(Vec<RwLock<Vec<Arc<SSTable>>>>, shared::SSTableId), shared::SimpleDbError> {
        let mut levels: Vec<RwLock<Vec<Arc<SSTable>>>> = Vec::with_capacity(64);
        for _ in 0..64 {
            levels.push(RwLock::new(Vec::new()));
        }

        let path = shared::get_directory_usize(&options.base_path, keyspace_id);
        let path = path.as_path();
        let mut max_sstable_id: shared::SSTableId = 0;

        for file in fs::read_dir(path).map_err(|e| shared::SimpleDbError::CannotReadSSTablesFiles(keyspace_id, e))? {
            let file = file.unwrap();

            if !is_sstable_file(&file) {
                continue;
            }

            if let Ok(sstable_id) = extract_sstable_id_from_file(&file) {
                println!("Loading SSTable with ID: {}", sstable_id);

                let sstable = SSTable::from_file(
                    sstable_id, keyspace_id, file.path().as_path(), options.clone()
                )?;

                if sstable.state.load(Acquire) != SSTABLE_ACTIVE {
                    sstable.delete();
                }

                let lock: &RwLock<Vec<Arc<SSTable>>> = &levels[sstable.level as usize];
                let write_result = lock.write();
                write_result.unwrap().push(sstable);

                max_sstable_id = max(max_sstable_id, sstable_id);
            }
        }

        Ok((levels, max_sstable_id))
    }

    pub fn scan_from_level(&self, levels_id: &Vec<usize>) -> MergeIterator<SSTableIterator> {
        let mut iterators: Vec<Box<SSTableIterator>> = Vec::new();

        for level_id in levels_id {
            let lock = self.sstables[*level_id].read();
            let sstables_in_level = lock.as_ref().unwrap();

            for sstable in sstables_in_level.iter() {
                iterators.push(Box::new(SSTableIterator::create(sstable.clone(), &Transaction::none())))
            }
        }

        MergeIterator::create(iterators)
    }

    pub fn scan_from_key(&self, transaction: &Transaction, key: &Bytes) -> MergeIterator<SSTableIterator> {
        let mut iterators = self.create_iterators(transaction);
        for iterator in &mut iterators {
            iterator.seek(key, true);
        }
        MergeIterator::create(iterators)
    }

    pub fn scan_all(&self, transaction: &Transaction) -> MergeIterator<SSTableIterator> {
        let iterators = self.create_iterators(transaction);
        MergeIterator::create(iterators)
    }

    fn create_iterators(&self, transaction: &Transaction) -> Vec<Box<SSTableIterator>> {
        let mut iterators: Vec<Box<SSTableIterator>> = Vec::with_capacity(self.sstables.len());

        for sstables_in_level_lock in self.sstables.iter() {
            let lock_result = sstables_in_level_lock.read();
            let sstable_in_level = lock_result.as_ref().unwrap();

            for sstable in sstable_in_level.iter() {
                iterators.push(Box::new(SSTableIterator::create(sstable.clone(), transaction)));
            }
        }

        iterators
    }

    pub fn get(&self, key: &Bytes, transaction: &Transaction) -> Result<Option<bytes::Bytes>, shared::SimpleDbError> {
        for sstables_in_level_lock in self.sstables.iter() {
            let lock_result = sstables_in_level_lock.read();
            let sstable_in_level = lock_result.as_ref().unwrap();

            for sstable in sstable_in_level.iter() {
                match sstable.get(key, transaction)? {
                    Some(value) => return Ok(Some(value)),
                    None => continue
                }
            }
        }

        Ok(None)
    }

    pub fn has_has_txn_id_been_written(&self, txn_id: shared::TxnId) -> bool {
        for sstables_in_level_lock in self.sstables.iter() {
            let lock_result = sstables_in_level_lock.read();
            let sstable_in_level = lock_result.as_ref().unwrap();

            for sstable in sstable_in_level.iter() {
                if sstable.has_has_txn_id_been_written(txn_id) {
                    return true;
                }
            }
        }

        false
    }

    pub fn delete_all_sstables(&self, level_id: usize) {
        match self.sstables.get(level_id) {
            Some(lock) => {
                let mut lock_result = lock.write();
                let mut sstables = lock_result.as_mut().unwrap();
                sstables.clear();
            },
            None => {},
        };
    }

    pub fn contains_sstable_id(&self, sstable_id: shared::SSTableId) -> bool {
        for lock_sstables_level in &self.sstables {
            let read_lock_result = lock_sstables_level.read().unwrap();
            for sstable_in_level in read_lock_result.iter() {
                if sstable_in_level.sstable_id == sstable_id {
                    return true;
                }
            }
        }

        false
    }

    pub fn delete_sstables(&self, level: usize, sstables_id: Vec<shared::SSTableId>) -> Result<(), shared::SimpleDbError> {
        match self.sstables.get(level) {
            Some(sstables_lock) => {
                let mut lock_result = sstables_lock.write();
                let mut sstables_in_level = lock_result.as_mut().unwrap();
                let mut indexes_to_remove = Vec::new();

                for (current_index, current_sstable) in sstables_in_level.iter().enumerate() {
                    for sstable_to_delete in sstables_id.iter() {
                        if *sstable_to_delete == current_sstable.sstable_id {
                            indexes_to_remove.push(current_index);
                        }
                    }
                }

                for index_to_remove in indexes_to_remove.iter().rev() {
                    let mut sstable = sstables_in_level.remove(*index_to_remove);
                    sstable.delete()?;
                }

                Ok(())
            },
            None => Ok(())
        }
    }

    pub fn get_sstables(&self, level: usize) -> Vec<Arc<SSTable>> {
        match self.sstables.get(level) {
            Some(sstables) => sstables.read().unwrap().clone(),
            None => Vec::new(),
        }
    }

    pub fn get_level_size_bytes(&self, level_id: usize) -> usize {
        match self.sstables.get(level_id) {
            Some(sstables) => sstables.read().unwrap()
                .iter()
                .map(|it| it.size())
                .sum(),
            None => 0,
        }
    }

    pub fn get_sstables_id(&self, level: usize) -> Vec<shared::SSTableId> {
        match self.sstables.get(level) {
            Some(sstables) => sstables.read().unwrap()
                .iter()
                .map(|it| it.sstable_id)
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

    pub fn is_last_level(&self, level: usize) -> bool {
        level + 1  >= self.sstables.len()
    }

    pub fn flush_memtable_to_disk(&self, sstable_builder: SSTableBuilder) -> Result<usize, shared::SimpleDbError> {
        let sstable_id: usize = self.next_sstable_id.fetch_add(1, Relaxed);
        let flush_operation = self.manifest.append_operation(ManifestOperationContent::MemtableFlush(MemtableFlushManifestOperation{
            memtable_id: sstable_builder.get_memtable_id().unwrap(),
            sstable_id,
        }))?;

        let flush_result = self.do_flush_to_disk(sstable_builder, sstable_id);

        self.manifest.mark_as_completed(flush_operation)?;

        flush_result
    }

    pub fn flush_to_disk(&self, sstable_builder: SSTableBuilder) -> Result<usize, shared::SimpleDbError> {
        let sstable_id: usize = self.next_sstable_id.fetch_add(1, Relaxed);
        self.do_flush_to_disk(sstable_builder, sstable_id)
    }

    fn do_flush_to_disk(&self, sstable_builder: SSTableBuilder, sstable_id: shared::SSTableId) -> Result<usize, shared::SimpleDbError> {
        let sstable_build_result = sstable_builder.build(
            sstable_id,
            self.to_sstable_file_path(sstable_id, self.keyspace_id).as_path(),
        );

        match sstable_build_result {
            Ok(sstable_built) => {
                let sstables_in_level_lock = &self.sstables[sstable_built.level as usize];
                let mut lock_result = sstables_in_level_lock.write();
                let sstables_in_level = lock_result.as_mut().unwrap();
                sstables_in_level.push(Arc::new(sstable_built));
                Ok(sstable_id)
            },
            Err(e) => Err(e),
        }
    }

    fn to_sstable_file_path(&self, sstable_id: shared::SSTableId, keyspace_id: shared::KeyspaceId) -> PathBuf {
        shared::get_file_usize(&self.options.base_path, keyspace_id, to_sstable_file_name(sstable_id).as_str())
    }

    pub fn calculate_space_amplificacion(&self) -> usize {
        let last_level_space: usize = match self.sstables.last() {
            Some(l0_sstables) => {
                l0_sstables.read()
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|sstable| sstable.size())
                    .sum::<usize>()
            }
            None => 0,
        };
        let rest_space: usize = self.sstables[0..self.sstables.len() - 1].iter()
            .map(|level_lock| {
                level_lock.read()
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|sstable| sstable.size())
                    .sum::<usize>()
            })
            .sum();

        rest_space / last_level_space
    }
}