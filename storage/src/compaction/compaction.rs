use crate::compaction::simple_leveled::{create_simple_level_compaction_task, start_simple_leveled_compaction, SimpleLeveledCompactionTask};
use crate::compaction::tiered::{create_tiered_compaction_task, start_tiered_compaction, TieredCompactionTask};
use serde::{Deserialize, Serialize};
use crate::sst::sstables::SSTables;
use std::time::Duration;
use std::sync::Arc;
use shared::{Flag, Type};
use shared::logger::logger;
use shared::logger::SimpleDbLayer::StorageKeyspace;
use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
use crate::manifest::manifest::{Manifest, ManifestOperationContent};
use crate::transactions::transaction_manager::TransactionManager;

pub struct Compaction {
    transaction_manager: Arc<TransactionManager>,
    options: Arc<shared::SimpleDbOptions>,
    sstables: Arc<SSTables>,
    manifest: Arc<Manifest>,
    keyspace_desc: KeyspaceDescriptor,
}

struct CompactionThread {
    transaction_manager: Arc<TransactionManager>,
    options: Arc<shared::SimpleDbOptions>,
    sstables: Arc<SSTables>,
    manifest: Arc<Manifest>,
    keyspace_desc: KeyspaceDescriptor
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum CompactionTask {
    SimpleLeveled(SimpleLeveledCompactionTask),
    Tiered(TieredCompactionTask),
}

impl Compaction {
    pub fn create(
        transaction_manager: Arc<TransactionManager>,
        options: Arc<shared::SimpleDbOptions>,
        sstables: Arc<SSTables>,
        manifest: Arc<Manifest>,
        keyspace_desc: KeyspaceDescriptor,
    ) -> Arc<Compaction> {
        Arc::new(Compaction {
            transaction_manager: transaction_manager.clone(),
            options: options.clone(),
            sstables: sstables.clone(),
            manifest: manifest.clone(),
            keyspace_desc,
        })
    }

    pub fn start_compaction_thread(&self) {
        logger().info(StorageKeyspace(self.keyspace_desc.keyspace_id), "Starting compaction thread");

        let compaction_thread = CompactionThread {
            transaction_manager: self.transaction_manager.clone(),
            sstables: self.sstables.clone(),
            manifest: self.manifest.clone(),
            keyspace_desc: self.keyspace_desc,
            options: self.options.clone(),
        };

        std::thread::spawn(move || {
            compaction_thread.start_compactions();
        });
    }

    pub fn compact(&self, compaction_task: CompactionTask, key_type: Type) -> Result<(), shared::SimpleDbError> {
        match compaction_task {
            CompactionTask::SimpleLeveled(simple_leveled_task) => start_simple_leveled_compaction(
                simple_leveled_task, &self.transaction_manager, &self.options, &self.sstables, self.keyspace_desc
            ),
            CompactionTask::Tiered(tiered_task) => start_tiered_compaction(
                tiered_task, &self.transaction_manager, &self.options, &self.sstables, self.keyspace_desc
            ),
        }
    }
}

impl CompactionThread {
    fn start_compactions(&self) -> ! {
        loop {
            std::thread::sleep(Duration::from_millis(self.options.compaction_task_frequency_ms as u64));

            if let Some(compaction_task) = self.create_compaction_task() {
                let operation_id = self.manifest.append_operation(ManifestOperationContent::Compaction(compaction_task));

                if let Err(compaction_error) = self.compact(compaction_task) {
                    logger().error(StorageKeyspace(self.keyspace_desc.keyspace_id), &format!("Error while compacting: {:?}", compaction_error));
                }

                if let Ok(operation_id) = operation_id {
                    let _ = self.manifest.mark_as_completed(operation_id);
                }
            }
        }
    }

    pub fn create_compaction_task(&self) -> Option<CompactionTask> {
        match self.options.compaction_strategy {
            shared::CompactionStrategy::SimpleLeveled => {
                if let Some(compaction_task) = create_simple_level_compaction_task(
                    self.options.simple_leveled_compaction_options, &self.sstables
                ) {
                    return Some(CompactionTask::SimpleLeveled(compaction_task));
                }
            },
            shared::CompactionStrategy::Tiered => {
                if let Some(compaction_task) = create_tiered_compaction_task(
                    self.options.tiered_compaction_options, &self.sstables
                ) {
                    return Some(CompactionTask::Tiered(compaction_task));
                }
            },
        }

        None
    }

    fn compact(&self, compaction_task: CompactionTask) -> Result<(), shared::SimpleDbError> {
        match compaction_task {
            CompactionTask::SimpleLeveled(simple_leveled_task) => start_simple_leveled_compaction(
                simple_leveled_task, &self.transaction_manager, &self.options, &self.sstables, self.keyspace_desc
            ),
            CompactionTask::Tiered(tiered_task) => start_tiered_compaction(
                tiered_task, &self.transaction_manager, &self.options, &self.sstables, self.keyspace_desc,
            ),
        }
    }
}