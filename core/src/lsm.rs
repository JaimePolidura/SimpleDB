use crate::compaction::compaction::{Compaction, CompactionTask};
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::manifest::manifest::{Manifest, ManifestOperationContent, MemtableFlushManifestOperation};
use crate::memtables::memtable::{MemTable, MemtableIterator};
use crate::memtables::memtables::Memtables;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::sst::ssttable_iterator::SSTableIterator;
use crate::utils::merge_iterator::MergeIterator;
use crate::utils::storage_iterator::StorageIterator;
use crate::utils::two_merge_iterators::TwoMergeIterator;
use std::sync::Arc;

pub struct Lsm {
    memtables: Memtables,
    sstables: Arc<SSTables>,
    compaction: Arc<Compaction>,
    manifest: Arc<Manifest>,

    options: Arc<LsmOptions>,
}

pub fn new(lsm_options: Arc<LsmOptions>) -> Lsm {
    let manifest = Arc::new(Manifest::new(lsm_options.clone())
        .expect("Cannot open/create Manifest file"));
    let sstables = Arc::new(SSTables::open(lsm_options.clone(), manifest.clone())
        .expect("Failed to read SSTable"));

    let mut lsm = Lsm {
        compaction: Compaction::new(lsm_options.clone(), sstables.clone(), manifest.clone()),
        memtables: Memtables::create(lsm_options.clone()).expect("Failed to create Memtables"),
        options: lsm_options.clone(),
        sstables: sstables.clone(),
        manifest,
    };

    //Memtables are recovered when calling Memtables::create
    lsm.recover_from_manifest();
    lsm.compaction.start_compaction_thread();

    lsm
}

impl Lsm {
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
        let sstable_builder_ready: SSTableBuilder = MemTable::to_sst(self.options.clone(), memtable.clone());
        let sstable_id = self.sstables.flush_memtable_to_disk(sstable_builder_ready)?;
        memtable.set_flushed();
        println!("Flushed memtable with ID: {} to SSTable with ID: {}", memtable.get_id(), sstable_id);
        Ok(())
    }

    //TODO If lsm engine crash during recovering from manifest, we will likely lose some operations
    fn recover_from_manifest(&mut self) {
        let manifest_operations = self.manifest.read_uncompleted_operations()
            .expect("Cannot read Manifest");

        for manifest_operation in manifest_operations {
            match manifest_operation {
                ManifestOperationContent::MemtableFlush(memtable_flush) => self.restart_memtable_flush(memtable_flush),
                ManifestOperationContent::Compaction(compaction_task) => self.restart_compaction(compaction_task),
                _ => {}
            };
        }
    }

    fn restart_compaction(&self, compaction: CompactionTask) {
        self.compaction.compact(compaction);
    }

    fn restart_memtable_flush(&mut self, memtable_flush: MemtableFlushManifestOperation) {
        //If it contains the SSTable, it means the memtable flush was completed before marking the operation as completed
        if !self.sstables.contains_sstable_id(memtable_flush.sstable_id) {
            let memtable_to_flush = self.memtables.get_memtable_to_flush(memtable_flush.memtable_id)
                .unwrap();
            self.flush_memtable(memtable_to_flush);
        }
    }
}