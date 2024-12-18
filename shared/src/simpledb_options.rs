use std::sync::Arc;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use crate::{Flag, Type};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum CompactionStrategy {
    SimpleLeveled,
    Tiered,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum DurabilityLevel {
    Strong, //Writes to memtable after WAL entry has been written to disk (using fsync)
    Weak, //Writes to memtable without waiting for WAL write to complete
}

//a is before b, (example b has greater timestamp (txn_id))
pub type StorageValueMergerFn = fn(
    a: &Bytes,
    b: &Bytes,
    keyspace_flags: Flag,
    primary_key_type: Type
) -> StorageValueMergeResult;

#[derive(Clone, Serialize, Deserialize)]
pub struct SimpleDbOptions {
    //Common/Shared option
    #[serde(skip)]
    pub base_path: String,
    pub use_debug_logging: bool,

    //Server layer options
    pub server_password: String,
    pub server_port: u16,

    //DB Layer options
    pub sort_page_size_bytes: usize,

    //Storage engine layer options
    pub simple_leveled_compaction_options: SimpleLeveledCompactionOptions,
    #[serde(skip)]
    pub storage_value_merger: Option<StorageValueMergerFn>,
    pub tiered_compaction_options: TieredCompactionOptions,
    pub compaction_strategy: CompactionStrategy,
    pub compaction_task_frequency_ms: usize,
    pub n_cached_blocks_per_sstable: usize,
    pub durability_level: DurabilityLevel,
    pub memtable_max_size_bytes: usize,
    pub max_memtables_inactive: usize,
    pub bloom_filter_n_entries: usize,
    pub block_size_bytes: usize,
    pub sst_size_bytes: usize,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct TieredCompactionOptions {
    pub min_levels_trigger_size_ratio: usize,
    pub max_size_amplification: usize,
    pub size_ratio: usize,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionOptions {
    pub level0_file_num_compaction_trigger: usize,
    pub size_ratio_percent: usize,
    pub max_levels: usize,
}

pub enum StorageValueMergeResult {
    Ok(Bytes),
    DiscardPreviousKeepNew,
    DiscardPreviousAndNew,
}

impl Default for SimpleDbOptions {
    fn default() -> Self {
        SimpleDbOptions {
            simple_leveled_compaction_options: SimpleLeveledCompactionOptions::default(),
            tiered_compaction_options: TieredCompactionOptions::default(),
            compaction_strategy: CompactionStrategy::SimpleLeveled,
            durability_level: DurabilityLevel::Strong,
            base_path: String::from("ignored"),
            compaction_task_frequency_ms: 100, //100ms
            memtable_max_size_bytes: 1048576, //1Mb
            n_cached_blocks_per_sstable: 8, //Expect power of two
            bloom_filter_n_entries: 32768, //4kb of bloom filter so it fits in a page
            storage_value_merger: None,
            sst_size_bytes: 268435456, //256 MB ~ 64 blocks
            max_memtables_inactive: 8,
            sort_page_size_bytes: 4096, //Kb
            block_size_bytes: 4096, //4kb
            use_debug_logging: true,
            server_port: 8888,
            server_password: String::from("123456"),
        }
    }
}

pub fn start_simpledb_options_builder() -> SimpleDbOptionsBuilder {
    SimpleDbOptionsBuilder {
        options: SimpleDbOptions::default()
    }
}

pub fn start_simpledb_options_builder_from(options: &SimpleDbOptions) -> SimpleDbOptionsBuilder {
    SimpleDbOptionsBuilder {
        options: options.clone()
    }
}

pub struct SimpleDbOptionsBuilder {
    options: SimpleDbOptions,
}

impl SimpleDbOptionsBuilder {
    pub fn simple_leveled_compaction_options(&mut self, value: SimpleLeveledCompactionOptions) -> &mut SimpleDbOptionsBuilder {
        self.options.simple_leveled_compaction_options = value;
        self
    }

    pub fn tiered_compaction_options(&mut self, value: TieredCompactionOptions) -> &mut SimpleDbOptionsBuilder {
        self.options.tiered_compaction_options = value;
        self
    }

    pub fn storage_value_merger(&mut self, storage_value_merger_fn: StorageValueMergerFn) -> &mut SimpleDbOptionsBuilder {
        self.options.storage_value_merger = Some(storage_value_merger_fn);
        self
    }

    pub fn durability_level(&mut self, level: DurabilityLevel) -> &mut SimpleDbOptionsBuilder {
        self.options.durability_level = level;
        self
    }

    pub fn compaction_strategy(&mut self, value: CompactionStrategy) -> &mut SimpleDbOptionsBuilder {
        self.options.compaction_strategy = value;
        self
    }

    pub fn compaction_task_frequency_ms(&mut self, value: usize) -> &mut SimpleDbOptionsBuilder {
        self.options.compaction_task_frequency_ms = value;
        self
    }

    pub fn n_cached_blocks_per_sstable(&mut self, value: usize) -> &mut SimpleDbOptionsBuilder {
        self.options.n_cached_blocks_per_sstable = value;
        self
    }

    pub fn memtable_max_size_bytes(&mut self, value: usize) -> &mut SimpleDbOptionsBuilder {
        self.options.memtable_max_size_bytes = value;
        self
    }

    pub fn max_memtables_inactive(&mut self, value: usize) -> &mut SimpleDbOptionsBuilder {
        self.options.max_memtables_inactive = value;
        self
    }

    pub fn bloom_filter_n_entries(&mut self, value: usize) -> &mut SimpleDbOptionsBuilder {
        self.options.bloom_filter_n_entries = value;
        self
    }

    pub fn block_size_bytes(&mut self, value: usize) -> &mut SimpleDbOptionsBuilder {
        self.options.block_size_bytes = value;
        self
    }

    pub fn sst_size_bytes(&mut self, value: usize) -> &mut SimpleDbOptionsBuilder {
        self.options.sst_size_bytes = value;
        self
    }

    pub fn base_path(&mut self, value: &str) -> &mut SimpleDbOptionsBuilder {
        self.options.base_path = value.to_string();
        self
    }

    pub fn build_arc(&self) -> Arc<SimpleDbOptions> {
        Arc::new(self.options.clone())
    }

    pub fn build(&self) -> SimpleDbOptions {
        self.options.clone()
    }
}

impl Default for TieredCompactionOptions {
    fn default() -> Self {
        TieredCompactionOptions {
            max_size_amplification: 2,
            size_ratio: 2,
            min_levels_trigger_size_ratio: 3,
        }
    }
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