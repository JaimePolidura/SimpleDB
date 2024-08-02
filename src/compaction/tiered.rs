use std::sync::Arc;
use bytes::Bytes;
use crate::lsm_options::LsmOptions;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::utils::storage_iterator::StorageIterator;

#[derive(Clone, Copy)]
pub struct TieredCompactionOptions {
    pub max_size_amplificacion: usize,
    pub size_ratio: usize,
    pub min_levels_trigger_size_ratio: usize,
}

enum TieredCompactionTask {
    None,
    AmplificationRatioTrigger,
    SizeRatioTrigger(usize)
}

impl Default for TieredCompactionOptions {
    fn default() -> Self {
        TieredCompactionOptions {
            max_size_amplificacion: 2,
            size_ratio: 2,
            min_levels_trigger_size_ratio: 3,
        }
    }
}

pub(crate) fn can_compact_tiered_compaction(
    options: TieredCompactionOptions,
    sstables: &Arc<SSTables>
) -> bool {
    match create_tiered_compaction_task(options, sstables) {
        TieredCompactionTask::None => false,
        _ => true,
    }
}

pub(crate) fn start_tiered_compaction(
    options: &Arc<LsmOptions>,
    sstables: &Arc<SSTables>
) {
    match create_tiered_compaction_task(options.tiered_compaction_options, sstables) {
        TieredCompactionTask::AmplificationRatioTrigger => do_tiered_compaction(options, sstables, sstables.get_n_levels() - 1),
        TieredCompactionTask::SizeRatioTrigger(level_id) => do_tiered_compaction(options, sstables, level_id),
        TieredCompactionTask::None => {}
    };
}

fn do_tiered_compaction(
    options: &Arc<LsmOptions>,
    sstables: &Arc<SSTables>,
    max_level_id_to_compact: usize //Well compact from level 0 to max_level_id_to_compact (inclusive, inclusive)
) {
    let new_level = max_level_id_to_compact + 1;
    let levels_id_to_compact: Vec<usize> = (0..max_level_id_to_compact).into_iter().collect();
    let iterator = sstables.iter(&levels_id_to_compact);
    let mut new_sstable_builder = Some(SSTableBuilder::new(
        options.clone(), new_level as u32
    ));

    while iterator.has_next() {
        new_sstable_builder.as_mut().unwrap().add_entry(
            iterator.key().clone(),
            Bytes::copy_from_slice(iterator.value())
        );

        if new_sstable_builder.as_ref().unwrap().estimated_size_bytes() > options.sst_size_bytes {
            sstables.flush_to_disk(new_sstable_builder.take().unwrap())
                .unwrap();
            new_sstable_builder = Some(SSTableBuilder::new(options.clone(), new_level as u32));
        }
    }

    levels_id_to_compact.iter()
        .for_each(|level_id| sstables.delete_all_sstables(*level_id));
}

fn create_tiered_compaction_task(
    options: TieredCompactionOptions,
    sstables: &Arc<SSTables>
) -> TieredCompactionTask {
    if  sstables.calculate_space_amplificacion() >= options.max_size_amplificacion {
        return TieredCompactionTask::AmplificationRatioTrigger;
    }

    let mut prev_levels_size = 0;
    for level_id in 0..sstables.get_n_levels() {
        let current_level_size: usize = sstables.get_sstables(level_id)
            .iter().map(|sdstable| sdstable.size())
            .sum();

        if prev_levels_size / current_level_size >= options.size_ratio && level_id >= options.min_levels_trigger_size_ratio {
            return TieredCompactionTask::SizeRatioTrigger(level_id);
        }

        prev_levels_size = prev_levels_size + current_level_size;
    }

    TieredCompactionTask::None
}