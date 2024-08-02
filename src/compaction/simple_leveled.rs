use std::sync::Arc;
use bytes::Bytes;
use crate::lsm_options::LsmOptions;
use crate::sst::sstable::SSTable;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::sst::ssttable_iterator::SSTableIterator;
use crate::utils::merge_iterator::MergeIterator;
use crate::utils::storage_iterator::StorageIterator;

#[derive(Clone, Copy)]
pub struct SimpleLeveledCompactionOptions {
    pub level0_file_num_compaction_trigger: usize,
    pub size_ratio_percent: usize,
    pub max_levels: usize,
}

impl Default for SimpleLeveledCompactionOptions {
    fn default() -> Self {
        SimpleLeveledCompactionOptions {
            level0_file_num_compaction_trigger: 1,
            size_ratio_percent: 1,
            max_levels: 8,
        }
    }
}

pub(crate) fn can_compact_simple_leveled_compaction(
    options: SimpleLeveledCompactionOptions,
    sstables: &Arc<SSTables>
) -> bool {
    get_level_to_compact(options, sstables).is_some()
}

pub(crate) fn start_simple_leveled_compaction(
    options: &Arc<LsmOptions>,
    sstables: &Arc<SSTables>
) {
    while let Some(level_to_compact) = get_level_to_compact(options.simple_leveled_compaction_options, sstables) {
        if level_to_compact > options.simple_leveled_compaction_options.max_levels {
            break;
        }

        let sstables_id_in_next_level = sstables.get_sstables_id(level_to_compact + 1);
        let sstables_id_in_level = sstables.get_sstables_id(level_to_compact);

        let iterator = sstables.iter(&vec![level_to_compact, level_to_compact + 1]);
        let mut new_sstable_builder = Some(SSTableBuilder::new(
            options.clone(), (level_to_compact + 1) as u32
        ));

        while iterator.has_next() {
            new_sstable_builder.as_mut().unwrap().add_entry(
                iterator.key().clone(),
                Bytes::copy_from_slice(iterator.value())
            );

            if new_sstable_builder.as_ref().unwrap().estimated_size_bytes() > options.sst_size_bytes {
                sstables.flush_to_disk(new_sstable_builder.take().unwrap())
                    .unwrap();

                new_sstable_builder = Some(SSTableBuilder::new(
                    options.clone(), (level_to_compact + 1) as u32
                ));
            }
        }

        sstables.delete_sstables(level_to_compact + 1, sstables_id_in_next_level);
        sstables.delete_sstables(level_to_compact, sstables_id_in_level);
    }
}

fn get_level_to_compact(
    options: SimpleLeveledCompactionOptions,
    sstables: &Arc<SSTables>
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