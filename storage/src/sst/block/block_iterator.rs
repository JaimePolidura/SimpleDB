use std::sync::Arc;
use bytes::Bytes;
use crate::sst::block::block::Block;
use crate::key::Key;
use crate::utils::storage_iterator::StorageIterator;

pub struct BlockIterator {
    block: Arc<Block>,

    current_value: Option<Bytes>,
    current_key: Option<Key>,
    current_index: usize,
    current_items_iterated: usize,
}

impl BlockIterator {
    pub fn create(block: Arc<Block>) -> BlockIterator {
        BlockIterator {
            block,
            current_value: None,
            current_key: None,
            current_index: 0,
            current_items_iterated: 0,
        }
    }

    //Returns true if the key is contained in the block
    //Returns false if the key is out of bounds the block
    //Expect next() call after seek_key(), in order to get the seeked valuae
    pub fn seek_key(&mut self, key: &Key) -> bool {
        if !self.block.contains_key(key) {
            self.finish_iterator();
            return false;
        }

        let index = self.block.get_key_iterator_index(key.as_bytes());
        self.current_items_iterated = index + 1;
        self.current_index = index;

        true
    }

    fn finish_iterator(&mut self) {
        self.current_items_iterated = self.block.offsets.len();
        self.current_index = self.block.offsets.len();
        self.current_value = None;
        self.current_key = None;
    }
}

impl StorageIterator for BlockIterator {
    fn next(&mut self) -> bool {
        let has_next = self.has_next();

        if has_next {
            self.current_value = Some(self.block.get_value_by_index(self.current_index));
            self.current_key = Some(self.block.get_key_by_index(self.current_index));
            self.current_items_iterated = self.current_items_iterated + 1;
            self.current_index = self.current_index + 1;
        }

        has_next
    }

    fn has_next(&self) -> bool {
        self.block.offsets.len() > self.current_items_iterated
    }

    fn key(&self) -> &Key {
        self.current_key
            .as_ref()
            .expect("Illegal iterator state")
    }

    fn value(&self) -> &[u8] {
        self.current_value
            .as_ref()
            .expect("Illegal iterator state")
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use bytes::Bytes;
    use crate::sst::block::block_builder::BlockBuilder;
    use crate::sst::block::block_iterator::BlockIterator;
    use crate::key;
    use crate::utils::storage_iterator::StorageIterator;

    #[test]
    fn seek_key() {
        let mut block_builder = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()));
        block_builder.add_entry(key::create_from_str("B", 1), Bytes::from(vec![1, 2, 3]));
        block_builder.add_entry(key::create_from_str("D", 1), Bytes::from(vec![4, 5, 6]));
        block_builder.add_entry(key::create_from_str("E", 1), Bytes::from(vec![4, 5, 6]));
        let block = Arc::new(block_builder.build());

        // Start from the beggining [B, D, E] Seek: A
        let mut iterator = BlockIterator::create(block.clone());
        iterator.seek_key(&key::create_from_str("A", 1));
        assert!(!iterator.has_next());

        // Out of bounds [B, D, E] Seek: A
        let mut iterator = BlockIterator::create(block.clone());
        iterator.seek_key(&key::create_from_str("F", 1));
        assert!(!iterator.has_next());

        // Start from D [B, D, E] Seek: D
        let mut iterator = BlockIterator::create(block.clone());
        iterator.seek_key(&key::create_from_str("D", 1));
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("D", 1)));

        // Start from D [B, D, E] Seek: C
        let mut iterator = BlockIterator::create(block.clone());
        iterator.seek_key(&key::create_from_str("C", 1));
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("D", 1)));
    }

    #[test]
    fn next_has_next() {
        let mut block_builder = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()));
        block_builder.add_entry(key::create_from_str("Jaime", 1), Bytes::from(vec![1, 2, 3]));
        block_builder.add_entry(key::create_from_str("Pedro", 1), Bytes::from(vec![4, 5, 6]));
        let block = Arc::new(block_builder.build());

        let mut block_iterator = BlockIterator::create(block);

        assert!(block_iterator.has_next());
        block_iterator.next();

        assert!(block_iterator.key().eq(&key::create_from_str("Jaime", 1)));
        assert!(block_iterator.value().eq(&vec![1, 2, 3]));

        assert!(block_iterator.has_next());
        block_iterator.next();

        assert!(block_iterator.key().eq(&key::create_from_str("Pedro", 1)));
        assert!(block_iterator.value().eq(&vec![4, 5, 6]));

        assert!(!block_iterator.has_next());
        assert!(!block_iterator.next());
    }
}