use std::sync::Arc;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use crate::lsm_options::LsmOptions;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::utils::storage_iterator::StorageIterator;

#[derive(Clone, Copy)]
pub struct SimpleLeveledCompactionOptions {
    pub level0_file_num_compaction_trigger: usize,
    pub size_ratio_percent: usize,
    pub max_levels: usize,
}

#[derive(Serialize, Deserialize, Copy, Clone)]
pub struct SimpleLeveledCompactionTask {
    level: usize,
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


pub(crate) fn start_simple_leveled_compaction(
    compaction_task: SimpleLeveledCompactionTask,
    options: &Arc<LsmOptions>,
    sstables: &Arc<SSTables>
) {
    let level_to_compact = compaction_task.level;

    if level_to_compact > options.simple_leveled_compaction_options.max_levels {
        return;
    }

    let sstables_id_in_next_level = sstables.get_sstables_id(level_to_compact + 1);
    let sstables_id_in_level = sstables.get_sstables_id(level_to_compact);

    let mut iterator = sstables.iter(&vec![level_to_compact, level_to_compact + 1]);
    let mut new_sstable_builder = Some(SSTableBuilder::new(
        options.clone(), (level_to_compact + 1) as u32
    ));
    let mut new_sstables_id = Vec::new();

    while iterator.has_next() {
        iterator.next();

        new_sstable_builder.as_mut().unwrap().add_entry(
            iterator.key().clone(),
            Bytes::copy_from_slice(iterator.value())
        );

        if new_sstable_builder.as_ref().unwrap().estimated_size_bytes() > options.sst_size_bytes {
            let new_sstable_id: usize = sstables.flush_to_disk(new_sstable_builder.take().unwrap())
                .unwrap();
            new_sstables_id.push(new_sstable_id);

            new_sstable_builder = Some(SSTableBuilder::new(
                options.clone(), (level_to_compact + 1) as u32
            ));
        }
    }

    if new_sstable_builder.as_ref().unwrap().n_entries() > 0 {
        new_sstables_id.push(sstables.flush_to_disk(new_sstable_builder.take().unwrap())
            .unwrap());
    }

    println!("Compacted SSTables: {:?} in level {} with SSTables {:?} in level {}. Created SSTables {:?}",
             sstables_id_in_level, level_to_compact, sstables_id_in_next_level, level_to_compact + 1,
             new_sstables_id);

    sstables.delete_sstables(level_to_compact + 1, sstables_id_in_next_level);
    sstables.delete_sstables(level_to_compact, sstables_id_in_level);
}

pub(crate) fn create_simple_level_compaction_task(
    options: SimpleLeveledCompactionOptions,
    sstables: &Arc<SSTables>
) -> Option<SimpleLeveledCompactionTask> {
    //Trigger l0 to l1 compaction
    if sstables.get_n_sstables(0) > options.level0_file_num_compaction_trigger {
        return Some(SimpleLeveledCompactionTask{level: 0});
    }

    for current_level in 1..sstables.get_n_levels() {
        let prev_level = current_level - 1;
        let n_sstables_current_level = sstables.get_n_sstables(current_level);
        let n_sstables_prev_level = sstables.get_n_sstables(prev_level);

        if n_sstables_prev_level / n_sstables_current_level < options.size_ratio_percent {
            return Some(SimpleLeveledCompactionTask{level: prev_level});
        }
    }

    None
}