use std::sync::Arc;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use shared::{Flag, Type};
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::transactions::transaction_manager::TransactionManager;
use crate::utils::storage_engine_iterator::StorageEngineIterator;
use shared::iterators::storage_iterator::StorageIterator;
use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
use crate::utils::tombstone::TOMBSTONE;

#[derive(Serialize, Deserialize, Copy, Clone)]
pub enum TieredCompactionTask {
    AmplificationRatioTrigger,
    SizeRatioTrigger(usize)
}

pub(crate) fn start_tiered_compaction(
    task: TieredCompactionTask,
    transaction_manager: &Arc<TransactionManager>,
    options: &Arc<shared::SimpleDbOptions>,
    sstables: &Arc<SSTables>,
    keyspace_desc: KeyspaceDescriptor
) -> Result<(), shared::SimpleDbError> {
    match task {
        TieredCompactionTask::AmplificationRatioTrigger => {
            do_tiered_compaction(options, sstables, sstables.get_n_levels() - 1, transaction_manager, keyspace_desc)
        },
        TieredCompactionTask::SizeRatioTrigger(level_id) => {
            do_tiered_compaction(options, sstables, level_id, transaction_manager, keyspace_desc)
        },
    }
}

fn do_tiered_compaction(
    options: &Arc<shared::SimpleDbOptions>,
    sstables: &Arc<SSTables>,
    max_level_id_to_compact: usize, //Compact from level 0 to max_level_id_to_compact (inclusive, inclusive)
    transaction_manager: &Arc<TransactionManager>,
    keyspace_desc: KeyspaceDescriptor
) -> Result<(), shared::SimpleDbError> {
    let new_level = max_level_id_to_compact + 1;
    let is_new_level_last_level = sstables.is_last_level(new_level);
    let levels_id_to_compact: Vec<usize> = (0..max_level_id_to_compact).into_iter().collect();
    let mut iterator = StorageEngineIterator::create(
        keyspace_desc,
        options,
        sstables.scan_from_level(&levels_id_to_compact),
    );
    let mut new_sstable_builder = Some(SSTableBuilder::create(
        options.clone(), keyspace_desc, new_level as u32
    ));

    while iterator.has_next() {
        iterator.next();

        let key = iterator.key().clone();
        match transaction_manager.on_write_key(&key) {
            Ok(_) => {
                let value = iterator.value();
                let is_tombstone = value.eq(TOMBSTONE.as_ref());

                if is_new_level_last_level && is_tombstone {
                    //We remove tombstones in the last levels compactions
                    continue;
                }

                new_sstable_builder.as_mut().unwrap().add_entry(
                    key, Bytes::copy_from_slice(iterator.value())
                );

                if new_sstable_builder.as_ref().unwrap().estimated_size_bytes() > options.sst_size_bytes {
                    sstables.flush_to_disk(new_sstable_builder.take().unwrap())?;

                    new_sstable_builder = Some(
                        SSTableBuilder::create(options.clone(), keyspace_desc, new_level as u32)
                    );
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
    options: shared::TieredCompactionOptions,
    sstables: &Arc<SSTables>
) -> Option<TieredCompactionTask> {
    if  sstables.calculate_space_amplificacion() >= options.max_size_amplification {
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