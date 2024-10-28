use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
use crate::sst::block::block::Block;
use crate::sst::block::block_iterator::BlockIterator;
use crate::sst::block_metadata::BlockMetadata;
use crate::sst::sstable::SSTable;
use crate::transactions::transaction::Transaction;
use bytes::Bytes;
use shared::iterators::storage_iterator::StorageIterator;
use shared::key::Key;
use std::sync::Arc;

//This iterators fulfills:
// - The returned keys are readable/visible by the current transaction.
// - The returned key's bytes might be returned multiple times.
//
//   For example (bytes, txn_id): (A, 1), (A, 2), (A, 3) with iterator txn_id = 2,
//   the iterator will return: (A, 1) and (A, 2)
#[derive(Clone)]
pub struct SSTableIterator {
    transaction: Transaction,
    sstable: Arc<SSTable>,
    key_desc: KeyspaceDescriptor,

    pending_blocks: Vec<BlockMetadata>,
    current_block_metadata: Option<BlockMetadata>,
    current_block_iterator: Option<BlockIterator>,
    current_block_id: i32, //Index to SSTable block_metadata

    current_value: Option<Bytes>,
    current_key: Option<Key>,
}

impl SSTableIterator {
    pub fn create(
        sstable: Arc<SSTable>,
        transaction: &Transaction,
        key_desc: KeyspaceDescriptor
    ) -> SSTableIterator {
        SSTableIterator {
            pending_blocks: sstable.blocks.block_metadata.clone(),
            transaction: transaction.clone(),
            current_block_iterator: None,
            current_block_metadata: None,
            current_block_id: -1,
            current_value: None,
            current_key: None,
            key_desc,
            sstable,
        }
    }

    fn next_key_iterator(&mut self) -> bool {
        let mut advanced = false;

        if self.pending_blocks.len() > 0 && self.current_block_iterator.is_none() {
            self.next_block();
            advanced = self.next_key_iterator();
        } else if self.current_block_iterator.is_some() && self.pending_blocks.len() > 0 {
            advanced = self.current_block_iterator
                .as_mut()
                .unwrap()
                .next();

            if !advanced {
                self.next_block();
                advanced = self.next_key_iterator();
            }
        } else if self.current_block_iterator.is_some() && self.pending_blocks.is_empty() {
            advanced = self.current_block_iterator
                .as_mut()
                .unwrap()
                .next();
        }

        advanced
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
        self.current_block_iterator = Some(BlockIterator::create(block, self.key_desc));
    }

    fn load_block(&mut self, block_id: usize) -> Arc<Block> {
        self.sstable.blocks.load_block(block_id)
            .expect("Cannot load block")
    }

    fn finish_iterator(&mut self) {
        self.pending_blocks.clear();
        self.current_block_metadata = None;
        self.current_block_iterator = None;
    }

    fn read_current_overflow_value(&mut self, first_value: Bytes) -> Bytes {
        let mut final_bytes = first_value.to_vec();

        loop {
            self.next_block();
            let is_overflow = self.current_block_iterator.as_ref().unwrap().is_current_value_overflow();
            let current_value = self.current_block_iterator.as_ref().unwrap().value();
            final_bytes.extend(current_value);

            if !is_overflow {
                return Bytes::from(final_bytes);
            }
        }
    }
}

impl StorageIterator for SSTableIterator {
    fn next(&mut self) -> bool {
        loop {
            let advanced = self.next_key_iterator();
            let block_iterator = self.current_block_iterator.as_ref().unwrap();

            if advanced && self.transaction.can_read(block_iterator.key()) {
                //Key
                self.current_key = Some(block_iterator.key().clone());
                //Value
                let current_value = block_iterator.value().clone();
                let is_overflow = block_iterator.is_current_value_overflow();

                if is_overflow {
                    self.current_value = Some(self.read_current_overflow_value(Bytes::copy_from_slice(current_value)));
                } else {
                    self.current_value = Some(Bytes::copy_from_slice(current_value));
                }

                return true
            } else if !advanced {
                return false;
            }
        }
    }

    fn has_next(&self) -> bool {
        !self.pending_blocks.is_empty() || (
            self.current_block_iterator.is_some() &&
            self.current_block_iterator.as_ref().expect("Illegal iterator state").has_next())
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

    fn seek(&mut self, key_bytes: &Bytes, inclusive: bool) {
        let key = Key::create(key_bytes.clone(), self.key_desc.key_type, 0);
        if (inclusive && self.sstable.key_greater(&key)) ||
            (!inclusive && self.sstable.key_greater_equal(&key)) {
            self.pending_blocks.clear();
            self.current_block_metadata = None;
            self.current_block_iterator = None;
            return;
        }
        if self.sstable.key_is_less_equal(&key) {
            return;
        }

        let mut some_block_seeked = false;

        while !self.pending_blocks.is_empty() {
            let current_block_metadata = self.pending_blocks.remove(0);
            self.current_block_id += 1;

            if current_block_metadata.contains(key_bytes, &self.transaction) {
                let current_block = self.load_block(self.current_block_id as usize);
                let mut current_block_iterator = BlockIterator::create(current_block, self.key_desc);

                current_block_iterator.seek(key_bytes, inclusive);

                self.current_block_metadata = Some(current_block_metadata);
                self.current_block_iterator = Some(current_block_iterator);
                some_block_seeked = true;
                break;
            }
        }

        if !some_block_seeked {
            self.finish_iterator();
        }
    }
}

#[cfg(test)]
mod test {
    use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
    use crate::sst::block::block_builder::BlockBuilder;
    use crate::sst::block_cache::BlockCache;
    use crate::sst::block_metadata::BlockMetadata;
    use crate::sst::sstable::{SSTable, SSTABLE_ACTIVE};
    use crate::sst::ssttable_iterator::SSTableIterator;
    use crate::transactions::transaction::Transaction;
    use crate::utils::bloom_filter::BloomFilter;
    use bytes::Bytes;
    use shared::iterators::storage_iterator::StorageIterator;
    use shared::key::Key;
    use shared::{assertions, SimpleDbFileWrapper, Type};
    use std::cell::UnsafeCell;
    use std::sync::atomic::AtomicU8;
    use std::sync::{Arc, Mutex};
    use crate::sst::block::blocks::Blocks;

    //SSTable:
    //Block1: [Alberto, Berto]
    //Block2: [Cigu, De]
    //Block3: [Estonia, Gibraltar, Zi]
    #[test]
    fn seek_start() {
        let mut iterator = build_sstable_iterator();
        iterator.seek(&Bytes::from("AAA"), true);

        assertions::assert_iterator_str_seq(
            iterator,
            vec![
                "Alberto",
                "Berto",
                "Cigu",
                "De",
                "Estonia",
                "Gibraltar",
                "Zi"
            ]
        );
    }

    //SSTable:
    //Block1: [Alberto, Berto]
    //Block2: [Cigu, De]
    //Block3: [Estonia, Gibraltar, Zi]
    #[test]
    fn seek_inclusive_contained() {
        let mut iterator = build_sstable_iterator();
        iterator.seek(&Bytes::from("Berto"), true);

        assertions::assert_iterator_str_seq(
            iterator,
            vec![
                "Berto",
                "Cigu",
                "De",
                "Estonia",
                "Gibraltar",
                "Zi"
            ]
        );
    }

    //SSTable:
    //Block1: [Alberto, Berto]
    //Block2: [Cigu, De]
    //Block3: [Estonia, Gibraltar, Zi]
    #[test]
    fn seek_exclusive_contained() {
        let mut iterator = build_sstable_iterator();
        iterator.seek(&Bytes::from("Cigu"), false);

        assertions::assert_iterator_str_seq(
            iterator,
            vec![
                "De",
                "Estonia",
                "Gibraltar",
                "Zi"
            ]
        );
    }

    //SSTable:
    //Block1: [Alberto, Berto]
    //Block2: [Cigu, De]
    //Block3: [Estonia, Gibraltar, Zi]
    #[test]
    fn next_has_next() {
        assertions::assert_iterator_str_seq(
            build_sstable_iterator(),
            vec![
                "Alberto",
                "Berto",
                "Cigu",
                "De",
                "Estonia",
                "Gibraltar",
                "Zi"
            ]
        );
    }

    fn build_sstable_iterator() -> SSTableIterator {
        let keyspace_desc = KeyspaceDescriptor::create_mock(Type::String);

        let mut block1 = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()), keyspace_desc);
        block1.add_entry(Key::create_from_str("Alberto", 0), Bytes::from(vec![1]));
        block1.add_entry(Key::create_from_str("Berto", 0), Bytes::from(vec![1]));
        let block1 = Arc::new(block1.build().remove(0));

        let mut block2 = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()), keyspace_desc);
        block2.add_entry(Key::create_from_str("Cigu", 0), Bytes::from(vec![1]));
        block2.add_entry(Key::create_from_str("De", 0), Bytes::from(vec![1]));
        let block2 = Arc::new(block2.build().remove(0));

        let mut block3 = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()), keyspace_desc);
        block3.add_entry(Key::create_from_str("Estonia", 0), Bytes::from(vec![1]));
        block3.add_entry(Key::create_from_str("Gibraltar", 0), Bytes::from(vec![1]));
        block3.add_entry(Key::create_from_str("Zi", 0), Bytes::from(vec![1]));
        let block3 = Arc::new(block3.build().remove(0));

        let mut block_cache = BlockCache::create(Arc::new(shared::SimpleDbOptions::default()));
        block_cache.put(0, block1);
        block_cache.put(1, block2);
        block_cache.put(2, block3);

        let sstable = Arc::new(SSTable{
            sstable_id: 1,
            bloom_filter: BloomFilter::create(&Vec::new(), 8),
            file: SimpleDbFileWrapper{ file: UnsafeCell::new(shared::SimpleDbFile::create_mock()) },
            blocks: Blocks {
                block_metadata: vec![
                    BlockMetadata{offset: 0, first_key: Key::create_from_str("Alberto", 0), last_key: Key::create_from_str("Berto", 0)},
                    BlockMetadata{offset: 8, first_key: Key::create_from_str("Cigu", 0), last_key: Key::create_from_str("De", 0)},
                    BlockMetadata{offset: 16, first_key: Key::create_from_str("Estonia", 0), last_key: Key::create_from_str("Zi", 0)},
                ],
                keyspace_desc: keyspace_desc.clone(),
                block_cache: Mutex::new(block_cache),
                options: Arc::new(shared::SimpleDbOptions::default()),
                file: SimpleDbFileWrapper{ file: UnsafeCell::new(shared::SimpleDbFile::create_mock()) },
                sstable_id
            },
            level: 0,
            state: AtomicU8::new(SSTABLE_ACTIVE),
            first_key: Key::create_from_str("Alberto", 1),
            last_key: Key::create_from_str("Zi", 1),
            keyspace_desc
        });

        SSTableIterator::create(sstable, &Transaction::none(), keyspace_desc)
    }
}