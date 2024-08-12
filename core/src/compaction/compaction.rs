use crate::compaction::simple_leveled::{create_simple_level_compaction_task, start_simple_leveled_compaction, SimpleLeveledCompactionTask};
use crate::compaction::tiered::{create_tiered_compaction_task, start_tiered_compaction, TieredCompactionTask};
use crate::lsm_options::{CompactionStrategy, LsmOptions};
use serde::{Deserialize, Serialize};
use crate::sst::sstables::SSTables;
use std::time::Duration;
use std::sync::Arc;
use crate::manifest::manifest::{Manifest, ManifestOperationContent};

pub struct Compaction {
    lsm_options: Arc<LsmOptions>,
    sstables: Arc<SSTables>,
    manifest: Arc<Manifest>
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum CompactionTask {
    SimpleLeveled(SimpleLeveledCompactionTask),
    Tiered(TieredCompactionTask),
}

impl Compaction {
    pub fn new(
        lsm_options: Arc<LsmOptions>,
        sstables: Arc<SSTables>,
        manifest: Arc<Manifest>,
    ) -> Arc<Compaction> {
        let compaction = Arc::new(Compaction {
            lsm_options: lsm_options.clone(),
            sstables: sstables.clone(),
            manifest: manifest.clone()
        });

        let compaction_cloned = compaction.clone();
        std::thread::spawn(move || {
            compaction_cloned.compaction_task();
        });

        compaction
    }

    fn compaction_task(&self) {
        loop {
            std::thread::sleep(Duration::from_millis(self.lsm_options.compaction_task_frequency_ms as u64));

            if let Some(compaction_task) = self.create_compaction_task() {
                let operation_id = self.manifest.append_operation(ManifestOperationContent::Compaction(compaction_task));

                self.compact(compaction_task);

                if let Ok(operation_id) = operation_id {
                    self.manifest.mark_as_completed(operation_id);
                }
            }
        }
    }

    pub fn create_compaction_task(&self) -> Option<CompactionTask> {
        match self.lsm_options.compaction_strategy {
            CompactionStrategy::SimpleLeveled => {
                if let Some(compaction_task) = create_simple_level_compaction_task(
                    self.lsm_options.simple_leveled_compaction_options, &self.sstables
                ) {
                    return Some(CompactionTask::SimpleLeveled(compaction_task));
                }
            },
            CompactionStrategy::Tiered => {
                if let Some(compaction_task) = create_tiered_compaction_task(
                    self.lsm_options.tiered_compaction_options, &self.sstables
                ) {
                    return Some(CompactionTask::Tiered(compaction_task));
                }
            },
        }

        None
    }

    pub fn compact(&self, compaction_task: CompactionTask) {
        match compaction_task {
            CompactionTask::SimpleLeveled(simpleLeveledTask) => start_simple_leveled_compaction(
                simpleLeveledTask, &self.lsm_options, &self.sstables
            ),
            CompactionTask::Tiered(tieredTask) => start_tiered_compaction(
                tieredTask, &self.lsm_options, &self.sstables
            ),
        }
    }
}