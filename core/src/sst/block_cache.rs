use std::cmp::{max, min};
use std::sync::Arc;
use crate::block::block::Block;
use crate::lsm_options::LsmOptions;

pub struct BlockCache {
    entries: Vec<Option<BlockCacheEntry>>,
}

struct BlockCacheEntry {
    block: Arc<Block>,
    block_id: usize,
    touched: i8,
}

impl BlockCache {
    pub fn new(lsm_options: Arc<LsmOptions>) -> BlockCache {
        let mut entries: Vec<Option<BlockCacheEntry>> = Vec::with_capacity(lsm_options.n_cached_blocks_per_sstable);
        for _ in 0..lsm_options.n_cached_blocks_per_sstable {
            entries.push(None);
        }

        BlockCache { entries }
    }

    pub fn put(&mut self, block_id: usize, block: Arc<Block>) {
        let index = self.get_index_to_put(block_id);

        self.entries[index] = Some(BlockCacheEntry {
            touched: 1,
            block,
            block_id,
        });
    }

    pub fn get(&mut self, block_id: usize) -> Option<Arc<Block>> {
        let mut current_index = self.get_index(block_id);

        for _ in 0..self.get_n_retries_read_cache() {
            let cache_entry: &mut Option<BlockCacheEntry> = &mut self.entries[current_index];

            if cache_entry.is_some() {
                let cache_entry = cache_entry.as_mut().unwrap();
                if cache_entry.block_id == block_id {
                    cache_entry.increase_touched();
                    return Some(cache_entry.block.clone());
                } else {
                    cache_entry.decrease_touched();
                }
            }

            current_index = self.next_index(current_index);
        }

        None
    }

    fn get_index_to_put(&mut self, block_id: usize) -> usize {
        let mut current_index = self.get_index(block_id);

        for _ in 0..self.get_n_retries_read_cache() {
            let current_entry = &mut self.entries[current_index];

            if current_entry.is_none() || current_entry.as_ref().unwrap().touched == 0 {
                return current_index;
            } else {
                let current_entry = current_entry.as_mut().unwrap();
                current_entry.decrease_touched();
            }

            current_index = self.next_index(current_index);
        }

        self.get_index(block_id)
    }

    fn next_index(&self, current_index: usize) -> usize {
        let next_index = current_index + 1;
        if self.entries.len() >= next_index {
            0
        } else {
            next_index
        }
    }

    fn get_n_retries_read_cache(&self) -> usize {
        self.entries.len() / 2
    }

    fn get_index(&self, block_id: usize) -> usize {
        block_id & (self.entries.len() - 1)
    }
}

impl BlockCacheEntry {
    pub fn increase_touched(&mut self) {
        self.touched = min(self.touched + 1, 2);
    }

    pub fn decrease_touched(&mut self) {
        self.touched = max(self.touched - 1, 0);
    }
}

#[cfg(test)]
mod test {
    use crate::block::block_builder::BlockBuilder;
    use crate::sst::block_cache::BlockCache;
    use crate::lsm_options::LsmOptions;
    use std::sync::Arc;

    #[test]
    fn put_get() {
        let block1 = Arc::new(BlockBuilder::new(Arc::new(LsmOptions::default())).build());
        let block2 = Arc::new(BlockBuilder::new(Arc::new(LsmOptions::default())).build());
        let block3 = Arc::new(BlockBuilder::new(Arc::new(LsmOptions::default())).build());
        let mut cache = BlockCache::new(Arc::new(LsmOptions::default()));

        cache.put(1, block1);
        cache.put(2, block2);
        cache.put(3, block3);

        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
        assert!(cache.get(1).is_some());
        assert!(cache.get(2).is_some());
        assert!(cache.get(3).is_some());
        assert!(cache.get(4).is_none());
    }
}