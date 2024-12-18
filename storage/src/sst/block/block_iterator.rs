use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
use crate::sst::block::block::Block;
use bytes::Bytes;
use shared::iterators::storage_iterator::StorageIterator;
use shared::key::Key;
use shared::MAX_TXN_ID;
use std::sync::Arc;

#[derive(Clone)]
pub struct BlockIterator {
    block: Arc<Block>,
    keyspace_desc: KeyspaceDescriptor,

    current_value: Option<Bytes>,
    is_current_value_overflow: bool,
    current_key: Option<Key>,
    current_index: usize,
    current_items_iterated: usize,
}

impl BlockIterator {
    pub fn create(block: Arc<Block>, keyspace_desc: KeyspaceDescriptor) -> BlockIterator {
        BlockIterator {
            keyspace_desc,
            block,
            current_value: None,
            is_current_value_overflow: false,
            current_key: None,
            current_index: 0,
            current_items_iterated: 0,
        }
    }

    pub fn is_current_value_overflow(&self) -> bool {
        self.is_current_value_overflow
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
            let (value, is_overflow) = self.block.get_value_by_index(self.current_index);
            self.is_current_value_overflow = is_overflow;
            self.current_value = Some(value);
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
        self.current_value.as_ref().expect("Illegal iterator state")
    }

    //Expect call before seek(), to make sure that the key is included in the block
    fn seek(&mut self, key_bytes: &Bytes, inclusive: bool) {
        let txn_id = if inclusive { MAX_TXN_ID } else { 0 };
        let key = &Key::create(key_bytes.clone(), self.keyspace_desc.key_type, txn_id);

        if self.block.is_key_bytes_higher(key, inclusive) {
            self.finish_iterator();
        } else if self.block.is_key_bytes_lower(key, inclusive) { //Start from beginning
            return;
        } else {
            let index = self.block.get_index(
                key_bytes,
                inclusive
            );
            self.current_items_iterated = index;
            self.current_index = index;
        }
    }

}

#[cfg(test)]
mod test {
    use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
    use crate::sst::block::block_builder::BlockBuilder;
    use crate::sst::block::block_iterator::BlockIterator;
    use bytes::Bytes;
    use shared::iterators::storage_iterator::StorageIterator;
    use shared::key::Key;
    use shared::{assertions, Type};
    use std::sync::Arc;

    #[test]
    fn seek_key() {
        let mut block_builder = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()), KeyspaceDescriptor::create_mock(Type::String));
        block_builder.add_entry(&Key::create_from_str("B", 1), &Bytes::from(vec![1, 2, 3]));
        block_builder.add_entry(&Key::create_from_str("D", 1), &Bytes::from(vec![4, 5, 6]));
        block_builder.add_entry(&Key::create_from_str("E", 1), &Bytes::from(vec![4, 5, 6]));
        let block = Arc::new(block_builder.build().remove(0));

        //[B, D, E] Seek: A, Inclusive
        let mut iterator = BlockIterator::create(block.clone(), KeyspaceDescriptor::create_mock(Type::String));
        iterator.seek(&Bytes::from("A"), true);
        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&Key::create_from_str("B", 1)));

        //[B, D, E] Seek: F, Inclusive
        let mut iterator = BlockIterator::create(block.clone(), KeyspaceDescriptor::create_mock(Type::String));
        iterator.seek(&Bytes::from("F"), true);
        assertions::assert_empty_iterator(iterator);

        //[B, D, E] Seek: D, Inclusive
        let mut iterator = BlockIterator::create(block.clone(), KeyspaceDescriptor::create_mock(Type::String));
        iterator.seek(&Bytes::from("D"), true);
        iterator.next();
        assert!(iterator.key().eq(&Key::create_from_str("D", 1)));

        //[B, D, E] Seek: D, Exclusive
        let mut iterator = BlockIterator::create(block.clone(), KeyspaceDescriptor::create_mock(Type::String));
        iterator.seek(&Bytes::from("D"), false);
        iterator.next();
        assert_eq!(*iterator.key(), Key::create_from_str("E", 1));

        //[B, D, E] Seek: C, Inclusive
        let mut iterator = BlockIterator::create(block.clone(), KeyspaceDescriptor::create_mock(Type::String));
        iterator.seek(&Bytes::from("C"), true);
        iterator.next();
        assert!(iterator.key().eq(&Key::create_from_str("D", 1)));

        //[B, D, E] Seek: E, Exclusive
        let mut iterator = BlockIterator::create(block.clone(), KeyspaceDescriptor::create_mock(Type::String));
        iterator.seek(&Bytes::from("E"), false);
        assertions::assert_empty_iterator(iterator);
    }

    #[test]
    fn next_has_next() {
        let mut block_builder = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()), KeyspaceDescriptor::create_mock(Type::String));
        block_builder.add_entry(&Key::create_from_str("A", 0), &Bytes::from(vec![1]));
        block_builder.add_entry(&Key::create_from_str("B", 0), &Bytes::from(vec![1]));
        block_builder.add_entry(&Key::create_from_str("C", 0), &Bytes::from(vec![1]));
        block_builder.add_entry(&Key::create_from_str("D", 0), &Bytes::from(vec![1]));
        block_builder.add_entry(&Key::create_from_str("E", 0), &Bytes::from(vec![1]));

        assertions::assert_iterator_str_seq(
            BlockIterator::create(Arc::new(block_builder.build().remove(0)), KeyspaceDescriptor::create_mock(Type::String)),
            vec!["A", "B", "C", "D", "E"]
        );
    }
}