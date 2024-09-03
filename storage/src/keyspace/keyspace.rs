use crate::compaction::compaction::{Compaction, CompactionTask};
use crate::lsm::LsmIterator;
use crate::lsm_error::LsmError;
use crate::lsm_options::LsmOptions;
use crate::manifest::manifest::{Manifest, ManifestOperationContent, MemtableFlushManifestOperation};
use crate::memtables::memtable::MemTable;
use crate::memtables::memtables::Memtables;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::transactions::transaction::{Transaction, TxnId};
use crate::transactions::transaction_manager::{IsolationLevel, TransactionManager};
use crate::utils::two_merge_iterators::TwoMergeIterator;
use std::sync::Arc;

pub type KeyspaceId = u16;

pub struct Keyspace {
    keyspace_id: KeyspaceId,
    transaction_manager: Arc<TransactionManager>,
    lsm_options: Arc<LsmOptions>,
    compaction: Arc<Compaction>,
    sstables: Arc<SSTables>,
    memtables: Memtables,
    manifest: Arc<Manifest>,
}

impl Keyspace {
    pub fn create(
        keyspace_id: KeyspaceId,
        transaction_manager: Arc<TransactionManager>,
        lsm_options: Arc<LsmOptions>
    ) -> Result<Arc<Keyspace>, LsmError> {
        let manifest = Arc::new(Manifest::create(lsm_options.clone(), keyspace_id)?);
        let sstables = Arc::new(SSTables::open(lsm_options.clone(), keyspace_id, manifest.clone())?);
        let memtables = Memtables::create_and_recover_from_wal(lsm_options.clone(), keyspace_id)?;
        let compaction =  Compaction::create(transaction_manager.clone(), lsm_options.clone(),
            sstables.clone(), manifest.clone());

        Ok(Arc::new(Keyspace{
            keyspace_id,
            transaction_manager,
            lsm_options,
            compaction,
            sstables,
            memtables,
            manifest
        }))
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

    pub fn get_with_transaction(
        &self,
        transaction: &Transaction,
        key: &str,
    ) -> Result<Option<bytes::Bytes>, LsmError> {
        match self.memtables.get(&key, transaction) {
            Some(value_from_memtable) => Ok(Some(value_from_memtable)),
            None => self.sstables.get(&key, &transaction),
        }
    }

    pub fn set_with_transaction(
        &self,
        transaction: &Transaction,
        key: &str,
        value: &[u8],
    ) -> Result<(), LsmError> {
        transaction.increase_nwrites();
        match self.memtables.set(&key, value, transaction) {
            Some(memtable_to_flush) => self.flush_memtable(memtable_to_flush),
            None => Ok(())
        }
    }

    pub fn delete(
        &self,
        key: &str
    ) -> Result<(), LsmError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::ReadUncommited);
        self.delete_with_transaction(&transaction, key)
    }

    pub fn delete_with_transaction(
        &self,
        transaction: &Transaction,
        key: &str,
    ) -> Result<(), LsmError> {
        transaction.increase_nwrites();
        match self.memtables.delete(&key, transaction) {
            Some(memtable_to_flush) => self.flush_memtable(memtable_to_flush),
            None => Ok(()),
        }
    }

    fn flush_memtable(&self, memtable: Arc<MemTable>) -> Result<(), LsmError> {
        let sstable_builder_ready: SSTableBuilder = memtable.to_sst(&self.transaction_manager);
        let sstable_id = self.sstables.flush_memtable_to_disk(sstable_builder_ready)?;
        memtable.set_flushed();
        println!("Flushed memtable with ID: {} to SSTable with ID: {}", memtable.get_id(), sstable_id);
        Ok(())
    }

    pub fn has_txn_id_been_written(&self, txn_id: TxnId) -> bool {
        if self.memtables.has_txn_id_been_written(txn_id) {
            return true;
        }

        self.sstables.has_has_txn_id_been_written(txn_id)
    }

    pub fn start_compaction_thread(&self) {
        self.compaction.start_compaction_thread();
    }

    //TODO If lsm engine crash during recovering from manifest, we will likely lose some operations
    pub fn recover_from_manifest(&self) {
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

    fn restart_memtable_flush(&self, memtable_flush: MemtableFlushManifestOperation) {
        //If it contains the SSTable, it means the memtable flush was completed before marking the operation as completed
        if !self.sstables.contains_sstable_id(memtable_flush.sstable_id) {
            let memtable_to_flush = self.memtables.get_memtable_to_flush(memtable_flush.memtable_id);
            if memtable_to_flush.is_some() {
                self.flush_memtable(memtable_to_flush.unwrap());
            }
        }
    }
}