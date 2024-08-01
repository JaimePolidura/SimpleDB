use std::sync::Arc;
use crate::compaction::simple_leveled::{can_compact_simple_leveled_compaction, SimpleLeveledOptions, start_simple_leveled_compaction};
use crate::lsm_options::LsmOptions;
use crate::sst::sstables::SSTables;

pub enum CompactionStrategy {
    SimpleLeveled(SimpleLeveledOptions),
}

pub fn can_compact(
    compaction_strategy: CompactionStrategy,
    sstables: &SSTables
) -> bool {
    match compaction_strategy {
        CompactionStrategy::SimpleLeveled(config) => can_compact_simple_leveled_compaction(config, sstables)
    }
}

pub fn compact(
    compaction_strategy: CompactionStrategy,
    lsm_options: &Arc<LsmOptions>,
    sstables: &mut SSTables
) {
    match compaction_strategy {
        CompactionStrategy::SimpleLeveled(config) => start_simple_leveled_compaction(lsm_options, sstables)
    }
}