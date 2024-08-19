[1mdiff --git a/core/src/lsm.rs b/core/src/lsm.rs[m
[1mindex d90cad3..145fad0 100644[m
[1m--- a/core/src/lsm.rs[m
[1m+++ b/core/src/lsm.rs[m
[36m@@ -35,19 +35,20 @@[m [mtype LsmIterator = TwoMergeIterator<MergeIterator<MemtableIterator>, MergeIterat[m
 pub fn new(lsm_options: Arc<LsmOptions>) -> Lsm {[m
     println!("Starting mini lsm engine!");[m
 [m
[31m-    let transaction_manager = Arc::new(TransactionManager::new(1));[m
     let manifest = Arc::new(Manifest::new(lsm_options.clone())[m
         .expect("Cannot open/create Manifest file"));[m
[31m-    let sstables = Arc::new(SSTables::open(transaction_manager.clone(), lsm_options.clone(), manifest.clone())[m
[32m+[m[32m    let sstables = Arc::new(SSTables::open(lsm_options.clone(), manifest.clone())[m
         .expect("Failed to read SSTable"));[m
[32m+[m[32m    let memtables = Memtables::new(lsm_options.clone())[m
[32m+[m[32m        .expect("Failed to create Memtables");[m
[32m+[m[32m    let transaction_manager = Arc::new(TransactionManager::new(1));[m
 [m
     let mut lsm = Lsm {[m
         compaction: Compaction::new(lsm_options.clone(), sstables.clone(), manifest.clone()),[m
[31m-        memtables: Memtables::new(transaction_manager.clone(), lsm_options.clone())[m
[31m-            .expect("Failed to create Memtables"),[m
[31m-        transacion_manager: Arc::new(TransactionManager::new(0)), //TODO[m
[32m+[m[32m        transacion_manager: Arc::new(transaction_manager), //TODO[m
         options: lsm_options.clone(),[m
         sstables: sstables.clone(),[m
[32m+[m[32m        memtables,[m
         manifest,[m
     };[m
 [m
[1mdiff --git a/core/src/memtables/memtable.rs b/core/src/memtables/memtable.rs[m
[1mindex b74e441..384d3a0 100644[m
[1m--- a/core/src/memtables/memtable.rs[m
[1m+++ b/core/src/memtables/memtable.rs[m
[36m@@ -10,6 +10,7 @@[m [muse crate::key::Key;[m
 use crate::lsm_error::LsmError;[m
 use crate::lsm_options::LsmOptions;[m
 use crate::memtables::memtable::MemtableState::{Active, Flushed, Flusing, Inactive, RecoveringFromWal};[m
[32m+[m[32muse crate::memtables::memtables::Memtables;[m
 use crate::memtables::wal::Wal;[m
 use crate::sst::sstable_builder::SSTableBuilder;[m
 use crate::transactions::transaction::Transaction;[m
[36m@@ -35,6 +36,11 @@[m [menum MemtableState {[m
     Flushed[m
 }[m
 [m
[32m+[m[32mpub struct MemtableCreated {[m
[32m+[m[32m    pub(crate) memtable: MemTable,[m
[32m+[m[32m    pub(crate) largest_txn_id: usize,[m
[32m+[m[32m}[m
[32m+[m
 impl MemTable {[m
     pub fn create_new([m
         options: Arc<LsmOptions>,[m
[36m@@ -68,7 +74,7 @@[m [mimpl MemTable {[m
         options: Arc<LsmOptions>,[m
         memtable_id: usize,[m
         wal: Wal[m
[31m-    ) -> Result<MemTable, LsmError> {[m
[32m+[m[32m    ) -> Result<MemtableCreated, LsmError> {[m
         let mut memtable = MemTable {[m
             max_size_bytes: options.memtable_max_size_bytes,[m
             current_size_bytes: AtomicUsize::new(0),[m
[36m@@ -79,7 +85,10 @@[m [mimpl MemTable {[m
         };[m
 [m
         memtable.recover_from_wal();[m
[31m-        Ok(memtable)[m
[32m+[m[32m        Ok(MemtableCreated{[m
[32m+[m[32m            memtable,[m
[32m+[m[32m            largest_txn_id: 0,[m
[32m+[m[32m        })[m
     }[m
 [m
     pub fn set_inactive(&self) {[m
[1mdiff --git a/core/src/memtables/memtables.rs b/core/src/memtables/memtables.rs[m
[1mindex 00393c7..7de5c8f 100644[m
[1m--- a/core/src/memtables/memtables.rs[m
[1m+++ b/core/src/memtables/memtables.rs[m
[36m@@ -6,30 +6,27 @@[m [muse crate::lsm_options::LsmOptions;[m
 use crate::memtables::memtable::{MemTable, MemtableIterator};[m
 use crate::memtables::wal::Wal;[m
 use crate::transactions::transaction::Transaction;[m
[31m-use crate::transactions::transaction_manager::{TransactionManager};[m
 use crate::utils::merge_iterator::MergeIterator;[m
 [m
 pub struct Memtables {[m
     inactive_memtables: AtomicPtr<RwLock<Vec<Arc<MemTable>>>>,[m
     current_memtable: AtomicPtr<Arc<MemTable>>,[m
 [m
[31m-    transaction_manager: Arc<TransactionManager>,[m
[31m-[m
     next_memtable_id: AtomicUsize,[m
     options: Arc<LsmOptions>,[m
 }[m
 [m
[32m+[m
 impl Memtables {[m
     pub fn new([m
[31m-        transaction_manager: Arc<TransactionManager>,[m
         options: Arc<LsmOptions>[m
     ) -> Result<Memtables, LsmError> {[m
         let (wals, max_memtable_id) = Wal::get_persisted_wal_id(&options)?;[m
 [m
         if !wals.is_empty() {[m
[31m-            Self::recover_memtables_from_wal(transaction_manager, options, max_memtable_id, wals)[m
[32m+[m[32m            Self::recover_memtables_from_wal(options, max_memtable_id, wals)[m
         } else {[m
[31m-            Self::create_memtables_no_wal(transaction_manager, options)[m
[32m+[m[32m            Self::create_memtables_no_wal(options)[m
         }[m
     }[m
 [m
[36m@@ -162,7 +159,6 @@[m [mimpl Memtables {[m
     }[m
 [m
     fn recover_memtables_from_wal([m
[31m-        transaction_manager: Arc<TransactionManager>,[m
         options: Arc<LsmOptions>,[m
         max_memtable_id: usize,[m
         wals: Vec<Wal>,[m
[36m@@ -173,32 +169,31 @@[m [mimpl Memtables {[m
 [m
         for wal in wals {[m
             let memtable_id = wal.get_memtable_id();[m
[31m-            let mut memtable = MemTable::create_and_recover_from_wal(options.clone(), memtable_id, wal)?;[m
[31m-            memtable.set_inactive();[m
[31m-            memtables.push(Arc::new(memtable));[m
[32m+[m[32m            let mut memtable_created = MemTable::create_and_recover_from_wal(options.clone(), memtable_id, wal)?;[m
[32m+[m
[32m+[m[32m            let memtable_created = memtable_created.memtable;[m
[32m+[m[32m            memtable_created.set_inactive();[m
[32m+[m[32m            memtables.push(Arc::new(memtable_created));[m
         }[m
 [m
         Ok(Memtables {[m
[31m-            current_memtable: AtomicPtr::new(Box::into_raw(Box::new(Arc::new(current_memtable)))),[m
             inactive_memtables: AtomicPtr::new(Box::into_raw(Box::new(RwLock::new(memtables)))),[m
[32m+[m[32m            current_memtable: AtomicPtr::new(Box::new(Arc::new(current_memtable))),[m
             next_memtable_id: AtomicUsize::new(next_memtable_id),[m
[31m-            transaction_manager,[m
             options[m
         })[m
     }[m
 [m
     fn create_memtables_no_wal([m
[31m-        transaction_manager: Arc<TransactionManager>,[m
         options: Arc<LsmOptions>[m
     ) -> Result<Memtables, LsmError> {[m
         let current_memtable = MemTable::create_new(options.clone(), 0)?;[m
         current_memtable.set_active();[m
 [m
         Ok(Memtables {[m
[31m-            inactive_memtables: AtomicPtr::new(Box::into_raw(Box::new(RwLock::new(Vec::with_capacity(options.max_memtables_inactive))))),[m
[32m+[m[32m            inactive_memtables: AtomicPtr::new(Box::new(RwLock::new(Vec::with_capacity(options.max_memtables_inactive)))),[m
             current_memtable: AtomicPtr::new(Box::into_raw(Box::new(Arc::new(current_memtable)))),[m
             next_memtable_id: AtomicUsize::new(1),[m
[31m-            transaction_manager,[m
             options[m
         })[m
     }[m
[1mdiff --git a/core/src/sst/sstables.rs b/core/src/sst/sstables.rs[m
[1mindex 30f11b4..ca41a00 100644[m
[1m--- a/core/src/sst/sstables.rs[m
[1m+++ b/core/src/sst/sstables.rs[m
[36m@@ -1,5 +1,4 @@[m
 use std::cmp::max;[m
[31m-use crate::key::Key;[m
 use crate::lsm_options::LsmOptions;[m
 use crate::sst::sstable::{SSTable, SSTABLE_ACTIVE};[m
 use crate::sst::sstable_builder::SSTableBuilder;[m
[36m@@ -14,10 +13,8 @@[m [muse std::sync::{Arc, RwLock};[m
 use crate::lsm_error::LsmError;[m
 use crate::manifest::manifest::{Manifest, ManifestOperationContent, MemtableFlushManifestOperation};[m
 use crate::transactions::transaction::Transaction;[m
[31m-use crate::transactions::transaction_manager::{TransactionManager};[m
 [m
 pub struct SSTables {[m
[31m-    transaction_manager: Arc<TransactionManager>,[m
     //For each level one index entry[m
     sstables: Vec<RwLock<Vec<Arc<SSTable>>>>,[m
     next_sstable_id: AtomicUsize,[m
[36m@@ -29,7 +26,6 @@[m [mpub struct SSTables {[m
 [m
 impl SSTables {[m
     pub fn open([m
[31m-        transaction_manager: Arc<TransactionManager>,[m
         lsm_options: Arc<LsmOptions>,[m
         manifest: Arc<Manifest>[m
     ) -> Result<SSTables, usize> {[m
[36m@@ -37,12 +33,11 @@[m [mimpl SSTables {[m
         for _ in 0..64 {[m
             levels.push(RwLock::new(Vec::new()));[m
         }[m
[31m-        let (sstables, max_ssatble_id) = Self::load_sstables(&lsm_options)?;[m
[32m+[m[32m        let (sstables, max_ssatble_id, largest_txn_id_loaded) = Self::load_sstables(&lsm_options)?;[m
 [m
[31m-        Ok(SSTables {[m
[32m+[m[32m        Ok(SSTables{[m
             next_sstable_id: AtomicUsize::new(max_ssatble_id + 1),[m
             path_buff: PathBuf::new(),[m
[31m-            transaction_manager,[m
             n_current_levels: 0,[m
             lsm_options,[m
             sstables,[m
[1mdiff --git a/core/src/transactions/mod.rs b/core/src/transactions/mod.rs[m
[1mindex 56a8e6b..2529e57 100644[m
[1m--- a/core/src/transactions/mod.rs[m
[1m+++ b/core/src/transactions/mod.rs[m
[36m@@ -1,2 +1,3 @@[m
 pub mod transaction_manager;[m
[31m-pub mod transaction;[m
\ No newline at end of file[m
[32m+[m[32mpub mod transaction;[m
[32m+[m[32mmod transaction_log;[m
\ No newline at end of file[m
[1mdiff --git a/core/src/transactions/transaction_log.rs b/core/src/transactions/transaction_log.rs[m
[1mindex e69de29..2f3533c 100644[m
[1m--- a/core/src/transactions/transaction_log.rs[m
[1m+++ b/core/src/transactions/transaction_log.rs[m
[36m@@ -0,0 +1,13 @@[m
[32m+[m[32muse std::sync::Arc;[m
[32m+[m[32muse crate::lsm_options::LsmOptions;[m
[32m+[m[32muse crate::utils::lsm_file::LsmFile;[m
[32m+[m
[32m+[m[32mpub struct TransactionLog {[m
[32m+[m[32m    log_file: LsmFile,[m
[32m+[m[32m}[m
[32m+[m
[32m+[m[32mimpl TransactionLog {[m
[32m+[m[32m    pub fn create(options: Arc<LsmOptions>) -> TransactionLog {[m
[32m+[m
[32m+[m[32m    }[m
[32m+[m[32m}[m
\ No newline at end of file[m
[1mdiff --git a/core/src/transactions/transaction_manager.rs b/core/src/transactions/transaction_manager.rs[m
[1mindex d98b947..d7948b6 100644[m
[1m--- a/core/src/transactions/transaction_manager.rs[m
[1m+++ b/core/src/transactions/transaction_manager.rs[m
[36m@@ -3,6 +3,7 @@[m [muse std::sync::atomic::AtomicU64;[m
 use std::sync::atomic::Ordering::Relaxed;[m
 use crossbeam_skiplist::SkipSet;[m
 use crate::transactions::transaction::Transaction;[m
[32m+[m[32muse crate::transactions::transaction_log::TransactionLog;[m
 [m
 #[derive(Clone)][m
 pub enum IsolationLevel {[m
[36m@@ -11,6 +12,8 @@[m [mpub enum IsolationLevel {[m
 }[m
 [m
 pub struct TransactionManager {[m
[32m+[m[32m    log: TransactionLog,[m
[32m+[m
     active_transactions: SkipSet<u64>,[m
     next_txn_id: AtomicU64,[m
 }[m
