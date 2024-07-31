use crate::sst::sstables::SSTables;

pub struct SimpleLeveledOptions {
    pub level0_file_num_compaction_trigger: usize,
    pub size_ratio_percent: usize,
    pub max_levels: usize,
}

impl Default for SimpleLeveledOptions {
    fn default() -> Self {
        SimpleLeveledOptions {
            level0_file_num_compaction_trigger: 1,
            size_ratio_percent: 1,
            max_levels: 8,
        }
    }
}

pub(crate) fn can_compact_simple_leveled_compaction(
    options: SimpleLeveledOptions,
    sstables: &SSTables
) -> bool {
    get_level_to_compact(options, sstables).is_some()
}

pub(crate) fn start_simple_leveled_compaction(
    options: SimpleLeveledOptions,
    sstables: &SSTables
) {

}

fn get_level_to_compact(
    options: SimpleLeveledOptions,
    sstables: &SSTables
) -> Option<usize> {
    //Trigger l0 to l1 compaction
    if sstables.get_n_sstables(0) >= options.level0_file_num_compaction_trigger {
        return Some(0);
    }

    for current_level in 1..sstables.get_n_levels() {
        let prev_level = current_level - 1;
        let n_sstables_current_level = sstables.get_n_sstables(current_level);
        let n_sstables_prev_level = sstables.get_n_sstables(prev_level);

        if n_sstables_prev_level / n_sstables_current_level < options.size_ratio_percent {
            return Some(prev_level);
        }
    }

    return None;
}