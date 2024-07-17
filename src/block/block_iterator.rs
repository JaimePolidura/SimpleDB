use bytes::Bytes;
use crate::block::block::Block;
use crate::key::Key;
use crate::utils::storage_iterator::StorageIterator;

pub struct BlockIterator {
    block: Block,

    current_value: Option<Bytes>,
    current_key: Option<Key>,
    current_offet_index: usize,
    current_items_iterated: usize,
}

impl BlockIterator {
    pub fn new(block: Block) -> BlockIterator {
        BlockIterator {
            block,
            current_value: None,
            current_key: None,
            current_offet_index: 0,
            current_items_iterated: 0,
        }
    }
}

impl StorageIterator for BlockIterator {
    fn next(&mut self) -> bool {
        let has_next = self.has_next();

        if has_next {
            self.current_items_iterated = self.current_items_iterated + 1;
            self.current_offet_index = self.current_offet_index + 1;

        }

        has_next
    }

    fn has_next(&self) -> bool {
        self.block.entries() > self.current_items_iterated
    }

    fn key(&self) -> &Key {
    }

    fn value(&self) -> &[u8] {
    }
}

