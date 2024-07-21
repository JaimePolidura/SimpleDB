use std::ptr::read_unaligned;
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

        panic!("Illegal next iterator state");
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