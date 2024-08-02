use std::sync::Arc;
use std::time::Duration;
use crate::compaction::simple_leveled::{can_compact_simple_leveled_compaction, start_simple_leveled_compaction};
use crate::compaction::tiered::{can_compact_tiered_compaction, start_tiered_compaction};
use crate::lsm_options::LsmOptions;
use crate::sst::sstables::SSTables;

pub struct Compaction {
    lsm_options: Arc<LsmOptions>,
    sstables: Arc<SSTables>,
}

pub enum CompactionStrategy {
    SimpleLeveled,
    Tiered,
}

impl Compaction {
    pub fn new(
        lsm_options: Arc<LsmOptions>,
        sstables: Arc<SSTables>,
    ) -> Arc<Compaction> {
        let compaction = Arc::new(Compaction {
            lsm_options: lsm_options.clone(),
            sstables: sstables.clone(),
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

            if self.can_compact() {
                self.compact();
            }
        }
    }

    pub fn can_compact(&self) -> bool {
        match self.lsm_options.compaction_strategy {
            CompactionStrategy::SimpleLeveled => can_compact_simple_leveled_compaction(
                self.lsm_options.simple_leveled_compaction_options, &self.sstables
            ),
            CompactionStrategy::Tiered => can_compact_tiered_compaction(
                self.lsm_options.tiered_compaction_options, &self.sstables
            ),
        }
    }

    pub fn compact(&self) {
        match self.lsm_options.compaction_strategy {
            CompactionStrategy::SimpleLeveled => start_simple_leveled_compaction(
                &self.lsm_options, &self.sstables
            ),
            CompactionStrategy::Tiered => start_tiered_compaction(
                &self.lsm_options, &self.sstables
            )
        }
    }
}