use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
use crate::sst::block::block::Block;
use crate::sst::block_cache::BlockCache;
use crate::sst::block_metadata::BlockMetadata;
use crate::transactions::transaction::Transaction;
use crate::utils::bloom_filter::BloomFilter;
use bytes::Bytes;
use shared::key::Key;
use shared::SimpleDbError::CannotDeleteSSTable;
use shared::{SimpleDbFile, SimpleDbFileWrapper};
use std::cell::UnsafeCell;
use std::path::Path;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Release;
use std::sync::{Arc, Mutex};

pub const SSTABLE_DELETED: u8 = 2;
pub const SSTABLE_ACTIVE: u8 = 1;

pub struct SSTable {
    pub(crate) sstable_id: shared::SSTableId,
    pub(crate) bloom_filter: BloomFilter,
    pub(crate) file: SimpleDbFileWrapper,
    pub(crate) block_cache: Mutex<BlockCache>,
    pub(crate) block_metadata: Vec<BlockMetadata>,
    pub(crate) options: Arc<shared::SimpleDbOptions>,
    pub(crate) level: u32,
    pub(crate) state: AtomicU8,
    pub(crate) first_key: Key,
    pub(crate) last_key: Key,

    pub(crate) keyspace_desc: KeyspaceDescriptor,
}

impl SSTable {
    pub fn create(
        block_metadata: Vec<BlockMetadata>,
        options: Arc<shared::SimpleDbOptions>,
        bloom_filter: BloomFilter,
        first_key: Key,
        last_key: Key,
        file: shared::SimpleDbFile,
        level: u32,
        sstable_id: shared::SSTableId,
        state: u8,
        keyspace_desc: KeyspaceDescriptor
    ) -> SSTable {
        SSTable {
            block_cache: Mutex::new(BlockCache::create(options.clone())),
            state: AtomicU8::new(state),
            file: SimpleDbFileWrapper {file: UnsafeCell::new(file)},
            block_metadata,
            bloom_filter,
            options,
            first_key,
            last_key,
            level,
            keyspace_desc,
            sstable_id,
        }
    }

    pub fn from_file(
        sstable_id: shared::SSTableId,
        path: &Path,
        options: Arc<shared::SimpleDbOptions>,
        keyspace_desc: KeyspaceDescriptor
    ) -> Result<Arc<SSTable>, shared::SimpleDbError> {
        let sst_file = shared::SimpleDbFile::open(path, shared::SimpleDbFileMode::RandomWrites)
            .map_err(|e| shared::SimpleDbError::CannotOpenSSTableFile(keyspace_desc.keyspace_id, sstable_id, e))?;
        let sst_bytes = sst_file.read_all()
            .map_err(|e| shared::SimpleDbError::CannotOpenSSTableFile(keyspace_desc.keyspace_id, sstable_id, e))?;

        Self::deserialize(&sst_bytes, sstable_id, options, sst_file, keyspace_desc)
    }

    fn deserialize(
        bytes: &Vec<u8>,
        sstable_id: shared::SSTableId,
        options: Arc<shared::SimpleDbOptions>,
        file: shared::SimpleDbFile,
        keyspace_desc: KeyspaceDescriptor
    ) -> Result<Arc<SSTable>, shared::SimpleDbError> {
        let meta_offset = shared::u8_vec_to_u32_le(bytes, bytes.len() - 4);
        let bloom_offset = shared::u8_vec_to_u32_le(bytes, bytes.len() - 8);
        let level = shared::u8_vec_to_u32_le(bytes, bytes.len() - 12);
        let state = bytes[bytes.len() - 13];

        let block_metadata = BlockMetadata::decode_all(bytes, meta_offset as usize, keyspace_desc.key_type)
            .map_err(|error_type| shared::SimpleDbError::CannotDecodeSSTable(
                keyspace_desc.keyspace_id,
                sstable_id,
                shared::SSTableCorruptedPart::BlockMetadata,
                shared::DecodeError {
                    offset: meta_offset as usize,
                    error_type,
                    index: 0,
                }
            ))?;

        let bloom_filter = BloomFilter::decode(bytes, bloom_offset as usize)
            .map_err(|error_type| shared::SimpleDbError::CannotDecodeSSTable(
                keyspace_desc.keyspace_id,
                sstable_id,
                shared::SSTableCorruptedPart::BloomFilter,
                shared::DecodeError {
                    offset: bloom_offset as usize,
                    error_type,
                    index: 0,
                }
            ))?;

        let first_key = Self::get_first_key(&block_metadata);
        let last_key = Self::get_last_key(&block_metadata);

        Ok(Arc::new(SSTable::create(
            block_metadata,
            options,
            bloom_filter,
            first_key,
            last_key,
            file,
            level,
            sstable_id,
            state,
            keyspace_desc
        )))
    }

    fn get_last_key(block_metadata: &Vec<BlockMetadata>) -> Key {
        block_metadata.get(block_metadata.len() - 1).unwrap().last_key.clone()
    }

    fn get_first_key(block_metadata: &Vec<BlockMetadata>) -> Key {
        block_metadata.get(0).unwrap().first_key.clone()
    }

    pub fn key_greater(&self, key: &Key) -> bool {
        self.last_key.lt(key)
    }

    pub fn key_greater_equal(&self, key: &Key) -> bool {
        self.last_key.lt(key)
    }

    pub fn key_is_less(&self, key: &Key) -> bool {
        self.first_key.gt(key)
    }

    pub fn key_is_less_equal(&self, key: &Key) -> bool {
        self.first_key.ge(key)
    }

    pub fn delete(&self) -> Result<(), shared::SimpleDbError> {
        self.state.store(SSTABLE_DELETED, Release);
        let file: &mut SimpleDbFile = unsafe { &mut *self.file.file.get() };
        file.delete()
            .map_err(|e| CannotDeleteSSTable(self.keyspace_desc.keyspace_id, self.sstable_id, e))
    }

    pub fn size(&self) -> shared::SSTableId {
        let file: &mut SimpleDbFile = unsafe { &mut *self.file.file.get() };
        file.size()
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
    
    pub fn get(&self, key: &Bytes, transaction: &Transaction) -> Result<Option<bytes::Bytes>, shared::SimpleDbError> {
        if self.first_key.bytes_gt_bytes(key) || self.last_key.bytes_lt_bytes(key) {
            return Ok(None);
        }
        if !self.bloom_filter.may_contain(shared::hash(key.as_ref())) {
            return Ok(None);
        }

        match self.get_blocks_metadata(key, transaction) {
            Some(block_metadata_index) => {
                let block = self.load_block(block_metadata_index)?;
                Ok(block.get_value(key, transaction))
            },
            None => Ok(None)
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
}