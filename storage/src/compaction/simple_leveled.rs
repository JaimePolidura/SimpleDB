use std::sync::Arc;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use shared::Flag;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::transactions::transaction_manager::TransactionManager;
use crate::utils::storage_engine_iterator::StorageEngineIterator;
use shared::iterators::storage_iterator::StorageIterator;
use crate::utils::tombstone::TOMBSTONE;

#[derive(Serialize, Deserialize, Copy, Clone)]
pub struct SimpleLeveledCompactionTask {
    level: usize,
}

pub(crate) fn start_simple_leveled_compaction(
    compaction_task: SimpleLeveledCompactionTask,
    transaction_manager: &Arc<TransactionManager>,
    options: &Arc<shared::SimpleDbOptions>,
    sstables: &Arc<SSTables>,
    keyspace_id: shared::KeyspaceId,
    keyspace_flags: Flag,
) -> Result<(), shared::SimpleDbError> {
    let level_to_compact = compaction_task.level;

    if level_to_compact > options.simple_leveled_compaction_options.max_levels {
        return Ok(());
    }

    let sstables_id_in_next_level = sstables.get_sstables_id(level_to_compact + 1);
    let sstables_id_in_level = sstables.get_sstables_id(level_to_compact);
    let is_new_level_last_level = sstables.is_last_level(level_to_compact + 1);
    let mut iterator = StorageEngineIterator::create(
        keyspace_flags,
        options,
        sstables.scan_from_level(&vec![level_to_compact, level_to_compact + 1]),
    );
    let mut new_sstable_builder = Some(SSTableBuilder::create(
        options.clone(), transaction_manager.clone(), keyspace_id, (level_to_compact + 1) as u32
    ));

    let mut new_sstables_id = Vec::new();

    while iterator.has_next() {
        iterator.next();

        let key = iterator.key().clone();

        match transaction_manager.on_write_key(&key) {
            Ok(_) => {
                let value = iterator.value().clone();
                let is_tombstone = value.eq(TOMBSTONE.as_ref());

                if is_new_level_last_level && is_tombstone {
                    //We remove tombstones in the last levels compactions
                    continue;
                }

                new_sstable_builder.as_mut().unwrap().add_entry(
                    key, Bytes::copy_from_slice(iterator.value())
                );

                if new_sstable_builder.as_ref().unwrap().estimated_size_bytes() > options.sst_size_bytes {
                    let new_sstable_id: usize = sstables.flush_to_disk(new_sstable_builder.take().unwrap())?;
                    new_sstables_id.push(new_sstable_id);

                    new_sstable_builder = Some(SSTableBuilder::create(
                        options.clone(), transaction_manager.clone(), keyspace_id, (level_to_compact + 1) as u32
                    ));
                }
            },
            Err(_) => {}
        };
    }

    if new_sstable_builder.as_ref().unwrap().n_entries() > 0 {
        new_sstables_id.push(sstables.flush_to_disk(new_sstable_builder.take().unwrap())?);
    }

    println!("Compacted SSTables: {:?} in level {} with SSTables {:?} in level {}. Created SSTables {:?}",
             sstables_id_in_level, level_to_compact, sstables_id_in_next_level, level_to_compact + 1,
             new_sstables_id);

    sstables.delete_sstables(level_to_compact + 1, sstables_id_in_next_level)?;
    sstables.delete_sstables(level_to_compact, sstables_id_in_level)?;

    Ok(())
}

pub(crate) fn create_simple_level_compaction_task(
    options: shared::SimpleLeveledCompactionOptions,
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