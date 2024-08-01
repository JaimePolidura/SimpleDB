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
    options: &Arc<LsmOptions>,
    sstables: &mut SSTables
) {
    while let Some(level_to_compact) = get_level_to_compact(options.simple_leveled_options, sstables) {
        let sstables_in_level_to_compact: Vec<Arc<SSTable>> = sstables.get_sstables(level_to_compact);
        let sstables_in_next_level: Vec<Arc<SSTable>> = sstables.get_sstables(level_to_compact + 1);

        let sstables_id_in_next_level = sstables.get_sstables_id(level_to_compact + 1);
        let sstables_id_in_level = sstables.get_sstables_id(level_to_compact);

        let iterator = create_merge_iterator(sstables_in_level_to_compact, sstables_in_next_level);
        let mut new_sstable_builder = Some(SSTableBuilder::new(
            options.clone(), (level_to_compact + 1) as u32
        ));
        let mut sstables_id_created: Vec<usize> = Vec::new();

        while iterator.has_next() {
            new_sstable_builder.as_mut().unwrap().add_entry(
                iterator.key().clone(),
                Bytes::copy_from_slice(iterator.value())
            );

            if new_sstable_builder.as_ref().unwrap().estimated_size_bytes() > options.sst_size_bytes {
                let sstable_id_created = sstables.flush_to_disk(new_sstable_builder.take().unwrap())
                    .unwrap();

                sstables_id_created.push(sstable_id_created);
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

fn create_merge_iterator(
    sstables_1: Vec<Arc<SSTable>>,
    sstables_2: Vec<Arc<SSTable>>,
) -> MergeIterator<SSTableIterator> {
    let mut iterators: Vec<Box<SSTableIterator>> = Vec::new();

    for sstable in sstables_1 {
        iterators.push(Box::new(SSTableIterator::new(sstable)));
    }
    for sstable in sstables_2 {
        iterators.push(Box::new(SSTableIterator::new(sstable)));
    }

    MergeIterator::new(iterators)
}