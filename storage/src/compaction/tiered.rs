use std::sync::Arc;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use crate::lsm_error::LsmError;
use crate::lsm_options::LsmOptions;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::transactions::transaction_manager::TransactionManager;
use crate::utils::storage_iterator::StorageIterator;

#[derive(Clone, Copy)]
pub struct TieredCompactionOptions {
    pub max_size_amplificacion: usize,
    pub size_ratio: usize,
    pub min_levels_trigger_size_ratio: usize,
}

#[derive(Serialize, Deserialize, Copy, Clone)]
pub enum TieredCompactionTask {
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

pub(crate) fn start_tiered_compaction(
    task: TieredCompactionTask,
    transaction_manager: &Arc<TransactionManager>,
    options: &Arc<LsmOptions>,
    sstables: &Arc<SSTables>
) -> Result<(), LsmError> {
    match task {
        TieredCompactionTask::AmplificationRatioTrigger => do_tiered_compaction(options, sstables, sstables.get_n_levels() - 1, transaction_manager),
        TieredCompactionTask::SizeRatioTrigger(level_id) => do_tiered_compaction(options, sstables, level_id, transaction_manager),
    }
}

fn do_tiered_compaction(
    options: &Arc<LsmOptions>,
    sstables: &Arc<SSTables>,
    max_level_id_to_compact: usize, //Compact from level 0 to max_level_id_to_compact (inclusive, inclusive)
    transaction_manager: &Arc<TransactionManager>
) -> Result<(), LsmError> {
    let new_level = max_level_id_to_compact + 1;
    let levels_id_to_compact: Vec<usize> = (0..max_level_id_to_compact).into_iter().collect();
    let mut iterator = sstables.iter(&levels_id_to_compact);
    let mut new_sstable_builder = Some(SSTableBuilder::new(
        options.clone(), new_level as u32
    ));

    while iterator.has_next() {
        iterator.next();

        let key = iterator.key().clone();
        match transaction_manager.on_write_key(&key) {
            Ok(_) => {
                new_sstable_builder.as_mut().unwrap().add_entry(
                    key, Bytes::copy_from_slice(iterator.value())
                );

                if new_sstable_builder.as_ref().unwrap().estimated_size_bytes() > options.sst_size_bytes {
                    sstables.flush_to_disk(new_sstable_builder.take().unwrap())?;

                    new_sstable_builder = Some(SSTableBuilder::new(options.clone(), new_level as u32));
                }
            },
            Err(_) => {}
        }
    }

    if new_sstable_builder.as_ref().unwrap().n_entries() > 0 {
        sstables.flush_to_disk(new_sstable_builder.take().unwrap())?;
    }

    levels_id_to_compact.iter()
        .for_each(|level_id| sstables.delete_all_sstables(*level_id));

    Ok(())
}

pub(crate) fn create_tiered_compaction_task(
    options: TieredCompactionOptions,
    sstables: &Arc<SSTables>
) -> Option<TieredCompactionTask> {
    if  sstables.calculate_space_amplificacion() >= options.max_size_amplificacion {
        return Some(TieredCompactionTask::AmplificationRatioTrigger);
    }

    let mut prev_levels_size = 0;
    for level_id in 0..sstables.get_n_levels() {
        let current_level_size: usize = sstables.get_sstables(level_id)
            .iter().map(|sdstable| sdstable.size())
            .sum();

        if prev_levels_size / current_level_size >= options.size_ratio && level_id >= options.min_levels_trigger_size_ratio {
            return Some(TieredCompactionTask::SizeRatioTrigger(level_id));
        }

        prev_levels_size = prev_levels_size + current_level_size;
    }

    None
}