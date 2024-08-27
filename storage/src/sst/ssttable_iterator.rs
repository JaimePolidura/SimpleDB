use std::sync::Arc;
use crate::sst::block::block::Block;
use crate::sst::block::block_iterator::BlockIterator;
use crate::key::Key;
use crate::sst::block_metadata::BlockMetadata;
use crate::sst::sstable::SSTable;
use crate::transactions::transaction::Transaction;
use crate::utils::storage_iterator::StorageIterator;

pub struct SSTableIterator {
    transaction: Transaction,
    sstable: Arc<SSTable>,

    pending_blocks: Vec<BlockMetadata>,
    current_block_metadata: Option<BlockMetadata>,
    current_block_iterator: Option<BlockIterator>,
    current_block_id: i32 //Index to SSTable block_metadata
}

impl SSTableIterator {
    pub fn new(sstable: Arc<SSTable>, transaction: &Transaction) -> SSTableIterator {
        SSTableIterator {
            pending_blocks: sstable.block_metadata.clone(),
            transaction: transaction.clone(),
            current_block_iterator: None,
            current_block_metadata: None,
            current_block_id: -1,
            sstable,
        }
    }

    fn set_iterator_as_empty(&mut self) {
        self.pending_blocks.clear();
        self.current_block_iterator = None;
        self.current_block_metadata = None;
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
        self.current_block_iterator = Some(BlockIterator::new(block));
    }

    fn load_block(&mut self, block_id: usize) -> Arc<Block> {
        self.sstable.load_block(block_id)
            .expect("Cannot load block")
    }
}

impl StorageIterator for SSTableIterator {
    fn next(&mut self) -> bool {
        loop {
            let mut advanced = self.next_key_iterator();
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
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::AtomicU8;
    use bytes::Bytes;
    use serde_json::ser::CharEscape::Tab;
    use crate::sst::block::block_builder::BlockBuilder;
    use crate::key;
    use crate::key::Key;
    use crate::lsm_options::LsmOptions;
    use crate::sst::block_cache::BlockCache;
    use crate::sst::block_metadata::BlockMetadata;
    use crate::sst::sstable::{SSTable, SSTABLE_ACTIVE};
    use crate::sst::ssttable_iterator::SSTableIterator;
    use crate::transactions::transaction::Transaction;
    use crate::utils::bloom_filter::BloomFilter;
    use crate::utils::lsm_file::LsmFile;
    use crate::utils::storage_iterator::StorageIterator;

    #[test]
    fn next_has_next() {
        let mut sstable_iteator: SSTableIterator = build_sstable_iterator();

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&key::new("Alberto", 1)));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&key::new("Berto", 1)));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&key::new("Cigu", 1)));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&key::new("De", 1)));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&key::new("Estonia", 1)));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&key::new("Gibraltar", 1)));

        assert!(sstable_iteator.has_next());
        sstable_iteator.next();
        assert!(sstable_iteator.key().eq(&key::new("Zi", 1)));

        assert!(!sstable_iteator.next());
        assert!(!sstable_iteator.has_next());
    }

    fn build_sstable_iterator() -> SSTableIterator {
        let mut block1 = BlockBuilder::new(Arc::new(LsmOptions::default()));
        block1.add_entry(key::new("Alberto", 1), Bytes::from(vec![1]));
        block1.add_entry(key::new("Berto", 1), Bytes::from(vec![1]));
        let block1 = Arc::new(block1.build());

        let mut block2 = BlockBuilder::new(Arc::new(LsmOptions::default()));
        block2.add_entry(key::new("Cigu", 1), Bytes::from(vec![1]));
        block2.add_entry(key::new("De", 1), Bytes::from(vec![1]));
        let block2 = Arc::new(block2.build());

        let mut block3 = BlockBuilder::new(Arc::new(LsmOptions::default()));
        block3.add_entry(key::new("Estonia", 1), Bytes::from(vec![1]));
        block3.add_entry(key::new("Gibraltar", 1), Bytes::from(vec![1]));
        block3.add_entry(key::new("Zi", 1), Bytes::from(vec![1]));
        let block3 = Arc::new(block3.build());

        let mut block_cache = BlockCache::new(Arc::new(LsmOptions::default()));
        block_cache.put(0, block1);
        block_cache.put(1, block2);
        block_cache.put(2, block3);

        let sstable = Arc::new(SSTable{
            id: 1,
            bloom_filter: BloomFilter::new(&Vec::new(), 8),
            file: LsmFile::mock(),
            block_cache: Mutex::new(block_cache),
            block_metadata: vec![
                BlockMetadata{offset: 0, first_key: key::new("Alberto", 1), last_key: key::new("Berto", 1)},
                BlockMetadata{offset: 8, first_key: key::new("Cigu", 1), last_key: key::new("De", 1)},
                BlockMetadata{offset: 16, first_key: key::new("Estonia", 1), last_key: key::new("Zi", 1)},
            ],
            lsm_options: Arc::new(LsmOptions::default()),
            level: 0,
            state: AtomicU8::new(SSTABLE_ACTIVE),
            first_key: key::new("Alberto", 1),
            last_key: key::new("Zi", 1),
        });

        SSTableIterator::new(sstable, &Transaction::none())
    }
}