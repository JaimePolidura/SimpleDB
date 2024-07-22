use std::sync::Arc;
use crate::block::block::Block;
use crate::block::block_iterator::BlockIterator;
use crate::key::Key;
use crate::sst::sstable::{BlockMetadata, SSTable};
use crate::utils::storage_iterator::StorageIterator;

pub struct SSTableIterator {
    sstable: Arc<SSTable>,

    pending_blocks: Vec<BlockMetadata>,
    current_block_metadata: Option<BlockMetadata>,
    current_block_iterator: Option<BlockIterator>,
    current_block_id: i32 //Index to SSTable block_metadata
}

impl SSTableIterator {
    pub fn new(sstable: Arc<SSTable>) -> SSTableIterator {
        SSTableIterator {
            pending_blocks: sstable.block_metadata.clone(),
            current_block_id: -1,
            current_block_iterator: None,
            current_block_metadata: None,
            sstable,
        }
    }

    pub fn seek_to_key(&mut self, key: &Key) {
        let mut right = self.pending_blocks.len() - 1;
        let mut left = 0;

        loop {
            let current_index = (left + right) / 2;
            let current_block_metadata = &self.pending_blocks[current_index];

            if left == right {
                self.set_iterator_as_empty();
                break
            }
            if current_block_metadata.contains(key) {
                self.current_block_id = current_index as i32;
                self.set_iterating_block(current_block_metadata.clone());
                break
            }
            if current_block_metadata.first_key.gt(key) {
                right = current_index;
            }
            if current_block_metadata.last_key.lt(key) {
                left = current_index;
            }
        }
    }

    fn set_iterator_as_empty(&mut self) {
        self.pending_blocks.clear();
        self.current_block_iterator = None;
        self.current_block_metadata = None;
    }

    fn next_block(&mut self) {
        if self.pending_blocks.len() > 0 {
            self.current_block_id = self.current_block_id + 1;
            let block_metadata = self.pending_blocks.remove(0);
            self.set_iterating_block(block_metadata);
        } else {
            self.current_block_metadata = None;
            self.current_block_iterator = None;
        }
    }

    fn set_iterating_block(&mut self, block_metadata: BlockMetadata) {
        let block = self.load_block(self.current_block_id as usize);
        self.current_block_metadata = Some(block_metadata);
        self.current_block_iterator = Some(BlockIterator::new(block));
    }

    fn load_block(&mut self, block_id: usize) -> Arc<Block> {
        self.sstable.load_block(block_id)
            .expect("Cannot load block")
    }
}

impl StorageIterator for SSTableIterator {
    fn next(&mut self) -> bool {
        if self.pending_blocks.len() > 0 && self.current_block_iterator.is_none() {
            self.next_block();
            self.next();
        } else if self.current_block_iterator.is_some() && self.pending_blocks.len() > 0 {
            let advanded = self.current_block_iterator
                .as_mut()
                .unwrap()
                .next();

            if !advanded {
                self.next_block();
                self.next();
            }
        } else if self.current_block_iterator.is_some() && self.pending_blocks.is_empty() {
            return self.current_block_iterator
                .as_mut()
                .unwrap()
                .next();
        }

        false
    }

    fn has_next(&self) -> bool {
        !self.pending_blocks.is_empty() || (
            self.current_block_iterator.is_some() &&
            self.current_block_iterator.as_ref().expect("Illegal iterator state").has_next())
    }

    fn key(&self) -> &Key {
        self.current_block_iterator
            .as_ref()
            .expect("Illegal iterator state")
            .key()
    }

    fn value(&self) -> &[u8] {
        self.current_block_iterator
            .as_ref()
            .expect("Illegal iterator state")
            .value()
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};
    use bytes::Bytes;
    use crate::block::block_builder::BlockBuilder;
    use crate::key::Key;
    use crate::lsm_options::LsmOptions;
    use crate::sst::block_cache::BlockCache;
    use crate::sst::sstable::{BlockMetadata, SSTable};
    use crate::sst::ssttable_iterator::SSTableIterator;
    use crate::utils::bloom_filter::BloomFilter;
    use crate::utils::lsm_file::LSMFile;
    use crate::utils::storage_iterator::StorageIterator;

    #[test]
    fn next_has_next() {
        let mut sstable_iteator: SSTableIterator = build_sstable_iterator();

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&Key::new("Alberto")));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&Key::new("Berto")));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&Key::new("Cigu")));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&Key::new("De")));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&Key::new("Estonia")));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&Key::new("Gibraltar")));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&Key::new("Zi")));

        assert!(!sstable_iteator.next());
        assert!(!sstable_iteator.has_next());
    }

    fn build_sstable_iterator() -> SSTableIterator {
        let mut block1 = BlockBuilder::new(LsmOptions::default());
        block1.add_entry(Key::new("Alberto"), Bytes::from(vec![1]));
        block1.add_entry(Key::new("Berto"), Bytes::from(vec![1]));
        let block1 = Arc::new(block1.build());

        let mut block2 = BlockBuilder::new(LsmOptions::default());
        block2.add_entry(Key::new("Cigu"), Bytes::from(vec![1]));
        block2.add_entry(Key::new("De"), Bytes::from(vec![1]));
        let block2 = Arc::new(block2.build());

        let mut block3 = BlockBuilder::new(LsmOptions::default());
        block3.add_entry(Key::new("Estonia"), Bytes::from(vec![1]));
        block3.add_entry(Key::new("Gibraltar"), Bytes::from(vec![1]));
        block3.add_entry(Key::new("Zi"), Bytes::from(vec![1]));
        let block3 = Arc::new(block3.build());

        let mut block_cache = BlockCache::new(LsmOptions::default());
        block_cache.put(0, block1);
        block_cache.put(1, block2);
        block_cache.put(2, block3);

        let sstable = Arc::new(SSTable{
            id: 1,
            bloom_filter: BloomFilter::new(&Vec::new(), 8),
            file: LSMFile::empty(),
            block_cache: Mutex::new(block_cache),
            block_metadata: vec![
                BlockMetadata{offset: 0, first_key: Key::new("Alberto"), last_key: Key::new("Berto")},
                BlockMetadata{offset: 8, first_key: Key::new("Cigu"), last_key: Key::new("De")},
                BlockMetadata{offset: 16, first_key: Key::new("Estonia"), last_key: Key::new("Zi")},
            ],
            lsm_options: LsmOptions::default()
        });

        SSTableIterator::new(sstable)
    }
}