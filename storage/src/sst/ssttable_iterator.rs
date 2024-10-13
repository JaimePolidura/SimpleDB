use crate::sst::block::block::Block;
use crate::sst::block::block_iterator::BlockIterator;
use crate::sst::block_metadata::BlockMetadata;
use crate::sst::sstable::SSTable;
use crate::transactions::transaction::Transaction;
use shared::iterators::storage_iterator::StorageIterator;
use bytes::Bytes;
use std::sync::Arc;
use shared::key::Key;

//This iterators fulfills:
// - The returned keys are readable/visible by the current transaction.
// - The returned key's bytes might be returned multiple times.
//
//   For example (bytes, txn_id): (A, 1), (A, 2), (A, 3) with iterator txn_id = 2,
//   the iterator will return: (A, 1) and (A, 2)
pub struct SSTableIterator {
    transaction: Transaction,
    sstable: Arc<SSTable>,

    pending_blocks: Vec<BlockMetadata>,
    current_block_metadata: Option<BlockMetadata>,
    current_block_iterator: Option<BlockIterator>,
    current_block_id: i32 //Index to SSTable block_metadata
}

impl SSTableIterator {
    pub fn create(sstable: Arc<SSTable>, transaction: &Transaction) -> SSTableIterator {
        SSTableIterator {
            pending_blocks: sstable.block_metadata.clone(),
            transaction: transaction.clone(),
            current_block_iterator: None,
            current_block_metadata: None,
            current_block_id: -1,
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
        self.current_block_iterator = Some(BlockIterator::create(block));
    }

    fn load_block(&mut self, block_id: usize) -> Arc<Block> {
        self.sstable.load_block(block_id)
            .expect("Cannot load block")
    }

    fn finish_iterator(&mut self) {
        self.pending_blocks.clear();
        self.current_block_metadata = None;
        self.current_block_iterator = None;
    }
}

impl StorageIterator for SSTableIterator {
    fn next(&mut self) -> bool {
        loop {
            let advanced = self.next_key_iterator();
            if advanced && self.transaction.can_read(self.key()) {
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

    fn seek(&mut self, key_bytes: &Bytes, inclusive: bool) {
        let key = Key::create(key_bytes.clone(), 0);
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
                let mut current_block_iterator = BlockIterator::create(current_block);

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
    use std::cell::UnsafeCell;
    use crate::sst::block::block_builder::BlockBuilder;
    use crate::sst::block_cache::BlockCache;
    use crate::sst::block_metadata::BlockMetadata;
    use crate::sst::sstable::{SSTable, SSTABLE_ACTIVE};
    use crate::sst::ssttable_iterator::SSTableIterator;
    use crate::transactions::transaction::Transaction;
    use crate::utils::bloom_filter::BloomFilter;
    use shared::iterators::storage_iterator::StorageIterator;
    use bytes::Bytes;
    use crossbeam_skiplist::SkipSet;
    use std::sync::atomic::AtomicU8;
    use std::sync::{Arc, Mutex};
    use shared::{assertions, SimpleDbFileWrapper};
    use shared::key::Key;

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
        let mut block1 = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()));
        block1.add_entry(Key::create_from_str("Alberto", 0), Bytes::from(vec![1]));
        block1.add_entry(Key::create_from_str("Berto", 0), Bytes::from(vec![1]));
        let block1 = Arc::new(block1.build());

        let mut block2 = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()));
        block2.add_entry(Key::create_from_str("Cigu", 0), Bytes::from(vec![1]));
        block2.add_entry(Key::create_from_str("De", 0), Bytes::from(vec![1]));
        let block2 = Arc::new(block2.build());

        let mut block3 = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()));
        block3.add_entry(Key::create_from_str("Estonia", 0), Bytes::from(vec![1]));
        block3.add_entry(Key::create_from_str("Gibraltar", 0), Bytes::from(vec![1]));
        block3.add_entry(Key::create_from_str("Zi", 0), Bytes::from(vec![1]));
        let block3 = Arc::new(block3.build());

        let mut block_cache = BlockCache::create(Arc::new(shared::SimpleDbOptions::default()));
        block_cache.put(0, block1);
        block_cache.put(1, block2);
        block_cache.put(2, block3);

        let sstable = Arc::new(SSTable{
            keyspace_id: 0,
            sstable_id: 1,
            bloom_filter: BloomFilter::create(&Vec::new(), 8),
            file: SimpleDbFileWrapper{ file: UnsafeCell::new(shared::SimpleDbFile::create_mock()) },
            block_cache: Mutex::new(block_cache),
            block_metadata: vec![
                BlockMetadata{offset: 0, first_key: Key::create_from_str("Alberto", 0), last_key: Key::create_from_str("Berto", 0)},
                BlockMetadata{offset: 8, first_key: Key::create_from_str("Cigu", 0), last_key: Key::create_from_str("De", 0)},
                BlockMetadata{offset: 16, first_key: Key::create_from_str("Estonia", 0), last_key: Key::create_from_str("Zi", 0)},
            ],
            options: Arc::new(shared::SimpleDbOptions::default()),
            level: 0,
            state: AtomicU8::new(SSTABLE_ACTIVE),
            first_key: Key::create_from_str("Alberto", 1),
            last_key: Key::create_from_str("Zi", 1),
        });

        SSTableIterator::create(sstable, &Transaction::none())
    }
}