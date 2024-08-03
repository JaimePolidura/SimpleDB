use std::sync::Arc;
use crate::lsm_options::LsmOptions;
use crate::sst::sstables::SSTables;

#[derive(Clone, Copy)]
pub struct LeveledCompactionOptions {
    level0_file_num_compaction_trigger: usize,
    base_level_size_bytes: usize,
    level_size_multiplier: usize,
    max_levels: usize,
}

impl Default for LeveledCompactionOptions {
    fn default() -> Self {
        LeveledCompactionOptions {
            level0_file_num_compaction_trigger: 5,
            base_level_size_bytes: 10485760, //10 MB
            level_size_multiplier: 10,
            max_levels: 8
        }
    }
}

pub(crate) fn can_compact_leveled_compaction(
    options: LeveledCompactionOptions,
    sstables: &Arc<SSTables>
) -> bool {

}

pub(crate) fn start_leveled_compaction(
    options: &Arc<LsmOptions>,
    sstables: &Arc<SSTables>
) {

}
