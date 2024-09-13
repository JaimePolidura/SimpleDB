use crate::compaction::compaction::{Compaction, CompactionTask};
use crate::manifest::manifest::{Manifest, ManifestOperationContent, MemtableFlushManifestOperation};
use crate::memtables::memtable::MemTable;
use crate::memtables::memtables::Memtables;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::transactions::transaction::Transaction;
use crate::transactions::transaction_manager::{IsolationLevel, TransactionManager};
use crate::utils::two_merge_iterators::TwoMergeIterator;
use crate::SimpleDbStorageIterator;
use bytes::Bytes;
use std::fs;
use std::sync::Arc;
use crate::utils::merge_values_iterator::MergeValuesIterator;

pub struct Keyspace {
    keyspace_id: shared::KeyspaceId,
    transaction_manager: Arc<TransactionManager>,
    options: Arc<shared::SimpleDbOptions>,
    compaction: Arc<Compaction>,
    sstables: Arc<SSTables>,
    memtables: Memtables,
    manifest: Arc<Manifest>,
}

impl Keyspace {
    pub fn create_new(
        keyspace_id: shared::KeyspaceId,
        transaction_manager: Arc<TransactionManager>,
        options: Arc<shared::SimpleDbOptions>
    ) -> Result<Arc<Keyspace>, shared::SimpleDbError> {
        let path = shared::get_directory_usize(&options.base_path, keyspace_id);
        fs::create_dir(path.as_path())
            .map_err(|e| shared::SimpleDbError::CannotCreateKeyspaceDirectory(keyspace_id, e))?;
        Self::create_and_load(keyspace_id, transaction_manager, options)
    }

    pub fn create_and_load(
        keyspace_id: shared::KeyspaceId,
        transaction_manager: Arc<TransactionManager>,
        options: Arc<shared::SimpleDbOptions>
    ) -> Result<Arc<Keyspace>, shared::SimpleDbError> {
        let manifest = Arc::new(Manifest::create(options.clone(), keyspace_id)?);
        let sstables = Arc::new(SSTables::open(options.clone(), keyspace_id, manifest.clone())?);
        let memtables = Memtables::create_and_recover_from_wal(options.clone(), keyspace_id)?;
        let compaction =  Compaction::create(transaction_manager.clone(), options.clone(),
                                             sstables.clone(), manifest.clone(), keyspace_id);

        Ok(Arc::new(Keyspace{
            keyspace_id,
            transaction_manager,
            options,
            compaction,
            sstables,
            memtables,
            manifest
        }))
    }

    pub fn scan_from_key_with_transaction(
        &self,
        transaction: &Transaction,
        key: &Bytes,
    ) -> SimpleDbStorageIterator {
        MergeValuesIterator::create(
            &self.options,
            TwoMergeIterator::create(
                self.memtables.scan_from_key(&transaction, key),
                self.sstables.scan_from_key(&transaction, key),
            )
        )
    }

    pub fn scan_all_with_transaction(
        &self,
        transaction: &Transaction
    ) -> SimpleDbStorageIterator {
        MergeValuesIterator::create(
            &self.options,
            TwoMergeIterator::create(
                self.memtables.scan_all(&transaction),
                self.sstables.scan_all(&transaction),
            )
        )
    }

    pub fn get_with_transaction(
        &self,
        transaction: &Transaction,
        key: &Bytes,
    ) -> Result<Option<Bytes>, shared::SimpleDbError> {
        match self.memtables.get(&key, transaction) {
            Some(value_from_memtable) => Ok(Some(value_from_memtable)),
            None => self.sstables.get(&key, &transaction),
        }
    }

    pub fn set_with_transaction(
        &self,
        transaction: &Transaction,
        key: Bytes,
        value: &[u8],
    ) -> Result<(), shared::SimpleDbError> {
        transaction.increase_nwrites();
        match self.memtables.set(key, value, transaction) {
            Some(memtable_to_flush) => self.flush_memtable(memtable_to_flush),
            None => Ok(())
        }
    }

    pub fn delete(
        &self,
        key: Bytes
    ) -> Result<(), shared::SimpleDbError> {
        let transaction = self.transaction_manager.start_transaction(IsolationLevel::ReadUncommited);
        self.delete_with_transaction(&transaction, key)
    }

    pub fn delete_with_transaction(
        &self,
        transaction: &Transaction,
        key: Bytes,
    ) -> Result<(), shared::SimpleDbError> {
        transaction.increase_nwrites();
        match self.memtables.delete(key, transaction) {
            Some(memtable_to_flush) => self.flush_memtable(memtable_to_flush),
            None => Ok(()),
        }
    }

    fn flush_memtable(&self, memtable: Arc<MemTable>) -> Result<(), shared::SimpleDbError> {
        let sstable_builder_ready: SSTableBuilder = memtable.to_sst(&self.transaction_manager);
        let sstable_id = self.sstables.flush_memtable_to_disk(sstable_builder_ready)?;
        memtable.set_flushed();
        println!("Flushed memtable with ID: {} to SSTable with ID: {}", memtable.get_id(), sstable_id);
        Ok(())
    }

    pub fn has_txn_id_been_written(&self, txn_id: shared::TxnId) -> bool {
        if self.memtables.has_txn_id_been_written(txn_id) {
            return true;
        }

        self.sstables.has_has_txn_id_been_written(txn_id)
    }

    pub fn start_compaction_thread(&self) {
        self.compaction.start_compaction_thread();
    }

    pub fn keyspace_id(&self) -> shared::KeyspaceId {
        self.keyspace_id
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