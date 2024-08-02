use std::sync::Arc;
use bytes::Bytes;
use crate::lsm_options::LsmOptions;
use crate::sst::sstable_builder::SSTableBuilder;
use crate::sst::sstables::SSTables;
use crate::sst::ssttable_iterator::SSTableIterator;
use crate::utils::storage_iterator::StorageIterator;

#[derive(Clone, Copy)]
pub struct TieredCompactionOptions {
    pub max_size_amplificacion: usize,
}

impl Default for TieredCompactionOptions {
    fn default() -> Self {
        TieredCompactionOptions {
            max_size_amplificacion: 2
        }
    }
}

pub(crate) fn can_compact_tiered_compaction(
    options: TieredCompactionOptions,
    sstables: &Arc<SSTables>
) -> bool {
    sstables.calculate_space_amplificacion() >= options.max_size_amplificacion
}

pub(crate) fn start_tiered_compaction(
    options: &Arc<LsmOptions>,
    sstables: &Arc<SSTables>
) {
    let new_level = sstables.get_n_levels();
    let levels_id_to_compact: Vec<usize> = (0..sstables.get_n_levels()).into_iter().collect();
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
