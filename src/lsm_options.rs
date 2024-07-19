#[derive(Copy, Clone)]
pub struct LsmOptions {
    pub memtable_max_size_bytes: usize,
    pub block_size_bytes: usize,
    pub sst_size_bytes: usize,
}

impl Default for LsmOptions {
    fn default() -> Self {
        LsmOptions {
            memtable_max_size_bytes: 1048576, //1Mb
            block_size_bytes: 4096, //4kb
            sst_size_bytes: 268435456, //256 MB
        }
    }
}