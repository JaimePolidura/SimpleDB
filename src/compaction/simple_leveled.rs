use crate::sst::sstables::SSTables;

pub struct SimpleLeveledOptions {
    pub level0_file_num_compaction_trigger: usize,
    pub size_ratio_percent: usize,
    pub max_levels: usize,
}

pub(crate) fn can_start_simple_leveled_compaction(
    options: SimpleLeveledOptions,
    sstables: &SSTables
) -> bool {
    true
}

pub(crate) fn start_simple_leveled_compaction(
    options: SimpleLeveledOptions,
    sstables: &SSTables
) {

}