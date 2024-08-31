use crate::key;
use crate::key::Key;
use crate::lsm_error::LsmError::{CannotDecodeSSTable, CannotDeleteSSTable, CannotOpenSSTableFile, CannotReadSSTableFile};
use crate::lsm_error::{DecodeError, LsmError, SSTableCorruptedPart};
use crate::lsm_options::LsmOptions;
use crate::sst::block::block::Block;
use crate::sst::block_cache::BlockCache;
use crate::sst::block_metadata::BlockMetadata;
use crate::transactions::transaction::{Transaction, TxnId};
use crate::utils::bloom_filter::BloomFilter;
use crate::utils::lsm_file::{LsmFile, LsmFileMode};
use crate::utils::utils;
use bytes::{Buf, BufMut};
use crossbeam_skiplist::SkipSet;
use std::path::Path;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Release;
use std::sync::{Arc, Mutex};

pub type SSTableId = usize;

pub const SSTABLE_DELETED: u8 = 2;
pub const SSTABLE_ACTIVE: u8 = 1;

pub struct SSTable {
    pub(crate) id: SSTableId,
    pub(crate) bloom_filter: BloomFilter,
    pub(crate) file: LsmFile,
    pub(crate) block_cache: Mutex<BlockCache>,
    pub(crate) block_metadata: Vec<BlockMetadata>,
    pub(crate) lsm_options: Arc<LsmOptions>,
    pub(crate) level: u32,
    pub(crate) state: AtomicU8,
    pub(crate) first_key: Key,
    pub(crate) last_key: Key,
    pub(crate) active_txn_ids_written: SkipSet<TxnId>
}

impl SSTable {
    pub fn new(
        active_txn_ids_written: SkipSet<TxnId>,
        block_metadata: Vec<BlockMetadata>,
        lsm_options: Arc<LsmOptions>,
        bloom_filter: BloomFilter,
        first_key: Key,
        last_key: Key,
        file: LsmFile,
        level: u32,
        id: SSTableId,
        state: u8,
    ) -> SSTable {
        SSTable {
            block_cache: Mutex::new(BlockCache::new(lsm_options.clone())),
            state: AtomicU8::new(state),
            active_txn_ids_written,
            block_metadata,
            bloom_filter,
            lsm_options,
            first_key,
            last_key,
            level,
            file,
            id,
        }
    }

    pub fn from_file(
        id: SSTableId,
        path: &Path,
        lsm_options: Arc<LsmOptions>
    ) -> Result<Arc<SSTable>, LsmError> {
        let sst_file = LsmFile::open(path, LsmFileMode::RandomWrites)
            .map_err(|e| CannotOpenSSTableFile(id, e))?;
        let sst_bytes = sst_file.read_all()
            .map_err(|e| CannotOpenSSTableFile(id, e))?;

        Self::decode(&sst_bytes, id, lsm_options, sst_file)
    }

    fn decode(
        bytes: &Vec<u8>,
        id: SSTableId,
        lsm_options: Arc<LsmOptions>,
        file: LsmFile,
    ) -> Result<Arc<SSTable>, LsmError> {
        let meta_offset = utils::u8_vec_to_u32_le(bytes, bytes.len() - 4);
        let bloom_offset = utils::u8_vec_to_u32_le(bytes, bytes.len() - 8);
        let active_txn_ids_written_offset = utils::u8_vec_to_u32_le(bytes, bytes.len() - 12);
        let level = utils::u8_vec_to_u32_le(bytes, bytes.len() - 16);
        let state = bytes[bytes.len() - 13];

        let block_metadata = BlockMetadata::decode_all(bytes, meta_offset as usize)
            .map_err(|error_type| CannotDecodeSSTable(id, SSTableCorruptedPart::BlockMetadata, DecodeError {
                offset: meta_offset as usize,
                path: file.path(),
                error_type,
                index: 0,
            }))?;

        let bloom_filter = BloomFilter::decode(bytes, bloom_offset as usize)
            .map_err(|error_type| CannotDecodeSSTable(id, SSTableCorruptedPart::BloomFilter, DecodeError {
                offset: bloom_offset as usize,
                path: file.path(),
                error_type,
                index: 0,
            }))?;

        let active_txn_ids_written = Self::decode_active_txn_ids_written(&bytes, active_txn_ids_written_offset);

        let first_key = Self::get_first_key(&block_metadata);
        let last_key = Self::get_last_key(&block_metadata);

        Ok(Arc::new(SSTable::new(
            active_txn_ids_written,
            block_metadata,
            lsm_options,
            bloom_filter,
            first_key,
            last_key,
            file,
            level,
            id,
            state
        )))
    }

    fn decode_active_txn_ids_written(bytes: &Vec<u8>, offset: u32) -> SkipSet<TxnId> {
        let mut decoded = SkipSet::new();
        let mut current_ptr: &[u8] = &bytes[offset];

        let n_entries = current_ptr.get_u32_le();
        for _ in 0..n_entries {
            let txn_id = current_ptr.get_u64_le() as TxnId;
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

    pub fn delete(&self) -> Result<(), LsmError> {
        self.state.store(SSTABLE_DELETED, Release);
        self.file.delete().map_err(|e| CannotDeleteSSTable(self.id, e))
    }

    pub fn size(&self) -> SSTableId {
        self.file.size()
    }

    pub fn load_block(&self, block_id: SSTableId) -> Result<Arc<Block>, LsmError> {
        {
            //Try read from cache
            let mut block_cache = self.block_cache.lock()
                .unwrap();
            let block_entry_from_cache = block_cache.get(block_id);

            if block_entry_from_cache.is_some() {
                return Ok::<Arc<Block>, LsmError>(block_entry_from_cache.unwrap());
            }
        }

        //Read from disk
        let metadata: &BlockMetadata = &self.block_metadata[block_id];
        let encoded_block = self.file.read(metadata.offset, self.lsm_options.block_size_bytes)
            .map_err(|e| CannotReadSSTableFile(self.id, e))?;

        let block = Block::decode(&encoded_block, &self.lsm_options)
            .map_err(|error_type| CannotDecodeSSTable(self.id, SSTableCorruptedPart::Block(block_id), DecodeError {
                offset: metadata.offset,
                path: self.file.path(),
                error_type,
                index: 0,
            }))?;

        let block = Arc::new(block);

        {
            //Write to cache
            let mut block_cache = self.block_cache.lock()
                .unwrap();
            block_cache.put(block_id, block.clone());
        }

        Ok(block)
    }
    
    pub fn get(&self, key: &str, transaction: &Transaction) -> Result<Option<bytes::Bytes>, LsmError> {
        if self.first_key.as_str().gt(key) || self.last_key.as_str().lt(key) {
            return Ok(None);
        }
        if !self.bloom_filter.may_contain(utils::hash(key.as_bytes())) {
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

    fn get_blocks_metadata(&self, key: &str, transaction: &Transaction) -> Option<usize> {
        let lookup_key = key::new(key, transaction.txn_id);
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
}