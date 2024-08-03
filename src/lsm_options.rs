use crate::compaction::compaction::CompactionStrategy;
use crate::compaction::leveled::LeveledCompactionOptions;
use crate::compaction::simple_leveled::SimpleLeveledCompactionOptions;
use crate::compaction::tiered::TieredCompactionOptions;

pub struct LsmOptions {
    pub simple_leveled_compaction_options: SimpleLeveledCompactionOptions,
    pub leveled_compaction_options: LeveledCompactionOptions,
    pub tiered_compaction_options: TieredCompactionOptions,
    pub compaction_strategy: CompactionStrategy,
    pub compaction_task_frequency_ms: usize,
    pub n_cached_blocks_per_sstable: usize,
    pub memtable_max_size_bytes: usize,
    pub max_memtables_inactive: usize,
    pub bloom_filter_n_entries: usize,
    pub block_size_bytes: usize,
    pub sst_size_bytes: usize,
    pub base_path: String,
}

impl Default for LsmOptions {
    fn default() -> Self {
        LsmOptions {
            simple_leveled_compaction_options: SimpleLeveledCompactionOptions::default(),
            leveled_compaction_options: LeveledCompactionOptions::default(),
            tiered_compaction_options: TieredCompactionOptions::default(),
            compaction_strategy: CompactionStrategy::SimpleLeveled,
            compaction_task_frequency_ms: 100, //100ms
            memtable_max_size_bytes: 1048576, //1Mb
            bloom_filter_n_entries: 32768, //4kb of bloom filter so it fits in a page
            block_size_bytes: 4096, //4kb
            sst_size_bytes: 268435456, //256 MB ~ 64 blocks
            n_cached_blocks_per_sstable: 8, //Expect power of two
            max_memtables_inactive: 8,
            base_path: String::from("ignored"),
        }
    }
}