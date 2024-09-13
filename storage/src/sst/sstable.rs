use crate::key;
use crate::key::Key;
use crate::sst::block::block::Block;
use crate::sst::block_cache::BlockCache;
use crate::sst::block_metadata::BlockMetadata;
use crate::transactions::transaction::{Transaction};
use crate::utils::bloom_filter::BloomFilter;
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipSet;
use std::path::Path;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Release;
use std::sync::{Arc, Mutex};

pub const SSTABLE_DELETED: u8 = 2;
pub const SSTABLE_ACTIVE: u8 = 1;

pub struct SSTable {
    pub(crate) sstable_id: shared::SSTableId,
    pub(crate) bloom_filter: BloomFilter,
    pub(crate) file: shared::SimpleDbFile,
    pub(crate) block_cache: Mutex<BlockCache>,
    pub(crate) block_metadata: Vec<BlockMetadata>,
    pub(crate) options: Arc<shared::SimpleDbOptions>,
    pub(crate) level: u32,
    pub(crate) state: AtomicU8,
    pub(crate) first_key: Key,
    pub(crate) last_key: Key,
    pub(crate) active_txn_ids_written: SkipSet<shared::TxnId>,

    pub(crate) keyspace_id: shared::KeyspaceId,
}

impl SSTable {
    pub fn create(
        active_txn_ids_written: SkipSet<shared::TxnId>,
        block_metadata: Vec<BlockMetadata>,
        options: Arc<shared::SimpleDbOptions>,
        bloom_filter: BloomFilter,
        first_key: Key,
        last_key: Key,
        file: shared::SimpleDbFile,
        level: u32,
        sstable_id: shared::SSTableId,
        state: u8,
        keyspace_id: shared::KeyspaceId
    ) -> SSTable {
        SSTable {
            block_cache: Mutex::new(BlockCache::create(options.clone())),
            state: AtomicU8::new(state),
            active_txn_ids_written,
            block_metadata,
            bloom_filter,
            options,
            first_key,
            last_key,
            level,
            file,
            sstable_id,
            keyspace_id,
        }
    }

    pub fn from_file(
        sstable_id: shared::SSTableId,
        keyspace_id: shared::KeyspaceId,
        path: &Path,
        options: Arc<shared::SimpleDbOptions>
    ) -> Result<Arc<SSTable>, shared::SimpleDbError> {
        let sst_file = shared::SimpleDbFile::open(path, shared::SimpleDbFileMode::RandomWrites)
            .map_err(|e| shared::SimpleDbError::CannotOpenSSTableFile(keyspace_id, sstable_id, e))?;
        let sst_bytes = sst_file.read_all()
            .map_err(|e| shared::SimpleDbError::CannotOpenSSTableFile(keyspace_id, sstable_id, e))?;

        Self::decode(&sst_bytes, sstable_id, keyspace_id, options, sst_file)
    }

    fn decode(
        bytes: &Vec<u8>,
        sstable_id: shared::SSTableId,
        keyspace_id: shared::KeyspaceId,
        options: Arc<shared::SimpleDbOptions>,
        file: shared::SimpleDbFile,
    ) -> Result<Arc<SSTable>, shared::SimpleDbError> {
        let meta_offset = shared::u8_vec_to_u32_le(bytes, bytes.len() - 4);
        let bloom_offset = shared::u8_vec_to_u32_le(bytes, bytes.len() - 8);
        let active_txn_ids_written_offset = shared::u8_vec_to_u32_le(bytes, bytes.len() - 12);
        let level = shared::u8_vec_to_u32_le(bytes, bytes.len() - 16);
        let state = bytes[bytes.len() - 13];

        let block_metadata = BlockMetadata::decode_all(bytes, meta_offset as usize)
            .map_err(|error_type| shared::SimpleDbError::CannotDecodeSSTable(
                keyspace_id,
                sstable_id,
                shared::SSTableCorruptedPart::BlockMetadata,
                shared::DecodeError {
                    offset: meta_offset as usize,
                    path: file.path(),
                    error_type,
                    index: 0,
                }
            ))?;

        let bloom_filter = BloomFilter::decode(bytes, bloom_offset as usize)
            .map_err(|error_type| shared::SimpleDbError::CannotDecodeSSTable(
                keyspace_id,
                sstable_id,
                shared::SSTableCorruptedPart::BloomFilter,
                shared::DecodeError {
                    offset: bloom_offset as usize,
                    path: file.path(),
                    error_type,
                    index: 0,
                }
            ))?;

        let active_txn_ids_written = Self::decode_active_txn_ids_written(&bytes, active_txn_ids_written_offset);

        let first_key = Self::get_first_key(&block_metadata);
        let last_key = Self::get_last_key(&block_metadata);

        Ok(Arc::new(SSTable::create(
            active_txn_ids_written,
            block_metadata,
            options,
            bloom_filter,
            first_key,
            last_key,
            file,
            level,
            sstable_id,
            state,
            keyspace_id
        )))
    }

    fn decode_active_txn_ids_written(bytes: &Vec<u8>, offset: u32) -> SkipSet<shared::TxnId> {
        let mut decoded = SkipSet::new();
        let mut current_ptr = &bytes[offset as usize..];

        let n_entries = current_ptr.get_u32_le();
        for _ in 0..n_entries {
            let txn_id = current_ptr.get_u64_le() as shared::TxnId;
            decoded.insert(txn_id);
        }

        decoded
    }

    fn get_last_key(block_metadata: &Vec<BlockMetadata>) -> Key {
        block_metadata.get(block_metadata.len() - 1).unwrap().last_key.clone()
    }

    fn get_first_key(block_metadata: &Vec<BlockMetadata>) -> Key {
        block_metadata.get(0).unwrap().first_key.clone()
    }

    pub fn is_key_higher(&self, key: &Key) -> bool {
        self.last_key.lt(key)
    }

    pub fn is_key_lower(&self, key: &Key) -> bool {
        self.first_key.gt(key)
    }

    pub fn delete(&self) -> Result<(), shared::SimpleDbError> {
        self.state.store(SSTABLE_DELETED, Release);
        self.file.delete().map_err(|e| shared::SimpleDbError::CannotDeleteSSTable(self.keyspace_id, self.sstable_id, e))
    }

    pub fn size(&self) -> shared::SSTableId {
        self.file.size()
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
        let encoded_block = self.file.read(metadata.offset, self.options.block_size_bytes)
            .map_err(|e| shared::SimpleDbError::CannotReadSSTableFile(self.keyspace_id, self.sstable_id, e))?;

        let block = Block::decode(&encoded_block, &self.options)
            .map_err(|error_type| shared::SimpleDbError::CannotDecodeSSTable(
                self.keyspace_id,
                self.sstable_id,
                shared::SSTableCorruptedPart::Block(block_id),
                shared::DecodeError {
                    offset: metadata.offset,
                    path: self.file.path(),
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
        let lookup_key = key::create(key.clone(), transaction.txn_id);
        let mut right = self.block_metadata.len() - 1;
        let mut left = 0;

        loop {
            let mut current_index = (left + right) / 2;
            let mut current_block_metadata = &self.block_metadata[current_index];

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

    pub fn has_has_txn_id_been_written(&self, txn_id: shared::TxnId) -> bool {
        self.active_txn_ids_written.contains(&txn_id)
    }
}