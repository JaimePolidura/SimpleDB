#[derive(Copy, Clone)]
pub struct LsmOptions {
    pub n_cached_blocks_per_sstable: usize,
    pub memtable_max_size_bytes: usize,
    pub bloom_filter_n_entries: usize,
    pub block_size_bytes: usize,
    pub sst_size_bytes: usize,
}

impl Default for LsmOptions {
    fn default() -> Self {
        LsmOptions {
            memtable_max_size_bytes: 1048576, //1Mb
            bloom_filter_n_entries: 32768, //4kb of bloom filter so it fits in a page
            block_size_bytes: 4096, //4kb
            sst_size_bytes: 268435456, //256 MB ~ 64 blocks
            n_cached_blocks_per_sstable: 8, //Expect power of two
        }
    }
}