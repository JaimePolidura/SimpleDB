use std::sync::{Arc, Mutex};
use bytes::Bytes;
use shared::{SSTableId, SimpleDbFile, SimpleDbFileWrapper, SimpleDbOptions};
use shared::key::Key;
use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
use crate::sst::block::block::Block;
use crate::sst::block_cache::BlockCache;
use crate::sst::block_metadata::BlockMetadata;
use crate::transactions::transaction::Transaction;

pub struct Blocks {
    pub(crate) block_metadata: Vec<BlockMetadata>,
    pub(crate) keyspace_desc: KeyspaceDescriptor,
    pub(crate) block_cache: Mutex<BlockCache>,
    pub(crate) options: Arc<SimpleDbOptions>,
    pub(crate) file: SimpleDbFileWrapper,
    pub(crate) sstable_id: SSTableId,
}

impl Blocks {
    pub fn create(
        keyspace_desc: KeyspaceDescriptor,
        block_metadata: Vec<BlockMetadata>,
        options: Arc<SimpleDbOptions>,
        file: SimpleDbFileWrapper,
        sstable_id: SSTableId
    ) -> Blocks {
        Blocks {
            block_cache: Mutex::new(BlockCache::create(options.clone())),
            keyspace_desc,
            block_metadata,
            sstable_id,
            options,
            file
        }
    }

    pub fn get(&self, key: &Bytes, transaction: &Transaction) -> Result<Option<bytes::Bytes>, shared::SimpleDbError> {
        match self.get_blocks_metadata(key, transaction) {
            Some(block_metadata_index) => {
                match self.load_block(block_metadata_index)?.get_value(key, transaction) {
                    Some((value, is_overflow)) => {
                        if is_overflow {
                            self.read_overflow_value(key, transaction, block_metadata_index, value)
                        } else {
                            Ok(Some(value))
                        }
                    }
                    None => Ok(None),
                }
            },
            None => Ok(None)
        }
    }

    fn read_overflow_value (
        &self,
        key: &Bytes,
        transaction: &Transaction,
        first_block_metadata_index: usize,
        first_block_value_bytes: Bytes
    ) -> Result<Option<bytes::Bytes>, shared::SimpleDbError> {
        let mut value_bytes = Vec::new();
        value_bytes.extend(first_block_value_bytes);
        let mut current_block_metadata_index = first_block_metadata_index + 1;

        loop {
            let block = self.load_block(current_block_metadata_index)?;
            current_block_metadata_index += 1;
            let (current_value, is_overflow) = block.get_value(key, transaction).unwrap();

            value_bytes.extend(current_value);

            if !is_overflow {
                return Ok(Some(Bytes::from(value_bytes)));
            }
        }
    }

    fn get_blocks_metadata(&self, key: &Bytes, transaction: &Transaction) -> Option<usize> {
        let lookup_key = Key::create(key.clone(), self.keyspace_desc.key_type, transaction.txn_id);
        let mut right = self.block_metadata.len() - 1;
        let mut left = 0;

        loop {
            let current_index = (left + right) / 2;
            let current_block_metadata = &self.block_metadata[current_index];

            if left == right {
                return None;
            }
            if current_block_metadata.contains(key, &transaction) {
                return Some(current_index);
            }
            if current_block_metadata.first_key.gt(&lookup_key) {
                right = current_index;
            }
            if current_block_metadata.last_key.lt(&lookup_key) {
                left = current_index;
            }
        }
    }

    pub fn load_block(&self, block_id: shared::SSTableId) -> Result<Arc<Block>, shared::SimpleDbError> {
        {
            //Try read from cache
            let mut block_cache = self.block_cache.lock()
                .unwrap();
            let block_entry_from_cache = block_cache.get(block_id);

            if block_entry_from_cache.is_some() {
                return Ok::<Arc<Block>, shared::SimpleDbError>(block_entry_from_cache.unwrap());
            }
        }

        //Read from disk
        let metadata: &BlockMetadata = &self.block_metadata[block_id];
        let file: &mut SimpleDbFile = unsafe { &mut *self.file.file.get() };
        let encoded_block = file.read(metadata.offset, self.options.block_size_bytes)
            .map_err(|e| shared::SimpleDbError::CannotReadSSTableFile(self.keyspace_desc.keyspace_id, self.sstable_id, e))?;

        let block = Block::deserialize(&encoded_block, &self.options, self.keyspace_desc)
            .map_err(|error_type| shared::SimpleDbError::CannotDecodeSSTable(
                self.keyspace_desc.keyspace_id,
                self.sstable_id,
                shared::SSTableCorruptedPart::Block(block_id),
                shared::DecodeError {
                    offset: metadata.offset,
                    error_type,
                    index: 0,
                }
            ))?;

        let block = Arc::new(block);

        {
            //Write to cache
            let mut block_cache = self.block_cache.lock()
                .unwrap();
            block_cache.put(block_id, block.clone());
        }

        Ok(block)
    }
}