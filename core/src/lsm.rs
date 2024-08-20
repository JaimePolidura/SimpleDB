use crate::compaction::compaction::{Compaction, CompactionTask};
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
use bytes::Bytes;
use crate::lsm_error::LsmError;
use crate::transactions::transaction::Transaction;
use crate::transactions::transaction_manager::{IsolationLevel, TransactionManager};

pub struct Lsm {
    transacion_manager: Arc<TransactionManager>,
    compaction: Arc<Compaction>,
    sstables: Arc<SSTables>,
    memtables: Memtables,
    manifest: Arc<Manifest>,

    options: Arc<LsmOptions>,
}

pub enum WriteBatch {
    Put(String, Bytes),
    Delete(String)
}

type LsmIterator = TwoMergeIterator<MergeIterator<MemtableIterator>, MergeIterator<SSTableIterator>>;

pub fn new(lsm_options: Arc<LsmOptions>) -> Lsm {
    println!("Starting mini lsm engine!");

    let manifest = Arc::new(Manifest::new(lsm_options.clone())
        .expect("Cannot open/create Manifest file"));
    let sstables = Arc::new(SSTables::open(lsm_options.clone(), manifest.clone())
        .expect("Failed to read SSTable"));
    let memtables = Memtables::new(lsm_options.clone())
        .expect("Failed to create Memtables");
    let transaction_manager = Arc::new(TransactionManager::create_recover_from_log());

    let mut lsm = Lsm {
        compaction: Compaction::new(lsm_options.clone(), sstables.clone(), manifest.clone()),
        transacion_manager: Arc::new(transaction_manager), //TODO
        options: lsm_options.clone(),
        sstables: sstables.clone(),
        memtables,
        manifest,
    };

    //Memtables are recovered when calling Memtables::create
    lsm.recover_from_manifest();
    lsm.compaction.start_compaction_thread();

    println!("Mini lsm engine started!");

    lsm
}

impl Lsm {
    pub fn scan_all(&self) -> LsmIterator {
        let transaction = self.transacion_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        self.scan_all_with_transaction(&transaction)
    }

    pub fn scan_all_with_transaction(
        &self,
        transaction: &Transaction
    ) -> LsmIterator {
        TwoMergeIterator::new(
            self.memtables.iterator(&transaction),
            self.sstables.iterator(&transaction),
        )
    }

    pub fn get(
        &self,
        key: &str
    ) -> Option<bytes::Bytes> {
        let transaction = self.transacion_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        self.get_with_transaction(&transaction, key)
    }

    pub fn get_with_transaction(
        &self,
        transaction: &Transaction,
        key: &str,
    ) -> Option<bytes::Bytes> {
        match self.memtables.get(&key, transaction) {
            Some(value_from_memtable) => Some(value_from_memtable),
            None => self.sstables.get(&key, &transaction),
        }
    }

    pub fn set(
        &self,
        key: &str,
        value: &[u8]
    ) -> Result<(), LsmError> {
        let transaction = self.transacion_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        self.set_with_transaction(&transaction, key, value)
    }

    pub fn set_with_transaction(
        &self,
        transaction: &Transaction,
        key: &str,
        value: &[u8],
    ) -> Result<(), LsmError> {
        match self.memtables.set(&key, value, transaction) {
            Some(memtable_to_flush) => self.flush_memtable(memtable_to_flush),
            None => Ok(())
        }
    }

    pub fn delete(
        &self,
        key: &str
    ) -> Result<(), LsmError> {
        let transaction = self.transacion_manager.start_transaction(IsolationLevel::ReadUncommited);
        self.delete_with_transaction(&transaction, key)
    }

    pub fn delete_with_transaction(
        &self,
        transaction: &Transaction,
        key: &str,
    ) -> Result<(), LsmError> {
        match self.memtables.delete(&key, transaction) {
            Some(memtable_to_flush) => self.flush_memtable(memtable_to_flush),
            None => Ok(()),
        }
    }

    pub fn write_batch(&self, batch: &[WriteBatch]) -> Result<(), LsmError> {
        let transaction = self.transacion_manager.start_transaction(IsolationLevel::SnapshotIsolation);
        for write_batch_record in batch {
            match write_batch_record {
                WriteBatch::Put(key, value) => self.set_with_transaction(&transaction, key.as_str(), value)?,
                WriteBatch::Delete(key) => self.delete_with_transaction(&transaction, key.as_str())?
            };
        }

        Ok(())
    }

    pub fn start_transaction(&self) -> Transaction {
        self.transacion_manager.start_transaction(IsolationLevel::SnapshotIsolation)
    }

    pub fn commit_transaction(&self, transaction: Transaction) {
        self.transacion_manager.commit(transaction);
    }

    fn flush_memtable(&self, memtable: Arc<MemTable>) -> Result<(), LsmError> {
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

        println!("Recovering {} operations from manifest", manifest_operations.len());

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
            let memtable_to_flush = self.memtables.get_memtable_to_flush(memtable_flush.memtable_id);
            if memtable_to_flush.is_some() {
                self.flush_memtable(memtable_to_flush.unwrap());
            }
        }
    }
}