use crate::block::block::Block;
use crate::block::block_iterator::BlockIterator;
use crate::key::Key;
use crate::sst::sstable::{BlockMetadata, SSTable};
use crate::utils::storage_iterator::StorageIterator;

pub struct SSTableIterator {
    sstable: SSTable,

    pending_blocks: Vec<BlockMetadata>,
    current_block_metadata: Option<BlockMetadata>,
    current_block_iterator: Option<BlockIterator>
}

impl SSTableIterator {
    pub fn new(sstable: SSTable) -> SSTableIterator {
        SSTableIterator {
            pending_blocks: sstable.block_metadata.iter().collect(),
            current_block_iterator: None,
            current_block_metadata: None,
            sstable,
        }
    }

    fn next_block(&mut self) {
        if self.pending_blocks.len() > 0 {
            let block_metadata = self.pending_blocks.remove(0);
            let block = self.load_block(&block_metadata);
            self.current_block_metadata = Some(block_metadata);
            self.current_block_iterator = Some(BlockIterator::new(block));
        } else {
            self.current_block_metadata = None;
            self.current_block_iterator = None;
        }
    }

    fn load_block(&self, block_metadata: &BlockMetadata) -> Block {
        self.sstable.load_block(block_metadata).expect("Cannot load block");
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
        } else if self.current_block_iterator.is_some() && self.pending_blocks.is_empty(){
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