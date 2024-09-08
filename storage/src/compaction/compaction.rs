use crate::compaction::simple_leveled::{create_simple_level_compaction_task, start_simple_leveled_compaction, SimpleLeveledCompactionTask};
use crate::compaction::tiered::{create_tiered_compaction_task, start_tiered_compaction, TieredCompactionTask};
use serde::{Deserialize, Serialize};
use crate::sst::sstables::SSTables;
use std::time::Duration;
use std::sync::Arc;
use crate::manifest::manifest::{Manifest, ManifestOperationContent};
use crate::transactions::transaction_manager::TransactionManager;

pub struct Compaction {
    transaction_manager: Arc<TransactionManager>,
    options: Arc<shared::SimpleDbOptions>,
    sstables: Arc<SSTables>,
    manifest: Arc<Manifest>,

    keyspace_id: shared::KeyspaceId,
}

struct CompactionThread {
    transaction_manager: Arc<TransactionManager>,
    options: Arc<shared::SimpleDbOptions>,
    sstables: Arc<SSTables>,
    manifest: Arc<Manifest>,

    keyspace_id: shared::KeyspaceId,
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
        keyspace_id: shared::KeyspaceId,
    ) -> Arc<Compaction> {
        Arc::new(Compaction {
            transaction_manager: transaction_manager.clone(),
            options: options.clone(),
            sstables: sstables.clone(),
            manifest: manifest.clone(),
            keyspace_id
        })
    }

    pub fn start_compaction_thread(&self) {
        println!("Starting compaction thread");

        let compaction_thread = CompactionThread {
            transaction_manager: self.transaction_manager.clone(),
            options: self.options.clone(),
            sstables: self.sstables.clone(),
            manifest: self.manifest.clone(),
            keyspace_id: self.keyspace_id,
        };

        std::thread::spawn(move || {
            compaction_thread.start_compactions();
        });
    }

    pub fn compact(&self, compaction_task: CompactionTask) -> Result<(), shared::SimpleDbError> {
        match compaction_task {
            CompactionTask::SimpleLeveled(simpleLeveledTask) => start_simple_leveled_compaction(
                simpleLeveledTask, &self.transaction_manager, &self.options, &self.sstables, self.keyspace_id,
            ),
            CompactionTask::Tiered(tieredTask) => start_tiered_compaction(
                tieredTask, &self.transaction_manager, &self.options, &self.sstables, self.keyspace_id,
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
                    println!("Error while compacting: {:?}", compaction_error);
                }

                if let Ok(operation_id) = operation_id {
                    self.manifest.mark_as_completed(operation_id)
                        .inspect_err(|e| println!("{:?}", e));
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
            CompactionTask::SimpleLeveled(simpleLeveledTask) => start_simple_leveled_compaction(
                simpleLeveledTask, &self.transaction_manager, &self.options, &self.sstables, self.keyspace_id
            ),
            CompactionTask::Tiered(tieredTask) => start_tiered_compaction(
                tieredTask, &self.transaction_manager, &self.options, &self.sstables, self.keyspace_id,
            ),
        }
    }
}