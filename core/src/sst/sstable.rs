use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};
use bytes::BufMut;
use crate::block::block::Block;
use crate::key;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::sst::block_cache::BlockCache;
use crate::utils::bloom_filter::BloomFilter;
use crate::utils::lsm_file::LsmFile;
use crate::utils::utils;

pub const SSTABLE_DELETED: u8 = 2;
pub const SSTABLE_ACTIVE: u8 = 1;

pub struct SSTable {
    pub(crate) id: usize,
    pub(crate) bloom_filter: BloomFilter,
    pub(crate) file: LsmFile,
    pub(crate) block_cache: Mutex<BlockCache>,
    pub(crate) block_metadata: Vec<BlockMetadata>,
    pub(crate) lsm_options: Arc<LsmOptions>,
    pub(crate) level: u32,
    pub(crate) state: AtomicU8,
    pub(crate) first_key: Key,
    pub(crate) last_key: Key,
}

impl SSTable {
    pub fn new(
        block_metadata: Vec<BlockMetadata>,
        lsm_options: Arc<LsmOptions>,
        bloom_filter: BloomFilter,
        first_key: Key,
        last_key: Key,
        file: LsmFile,
        level: u32,
        id: usize,
        state: u8,
    ) -> SSTable {
        SSTable {
            block_cache: Mutex::new(BlockCache::new(lsm_options.clone())),
            state: AtomicU8::new(state),
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
        id: usize,
        path: &Path,
        lsm_options: Arc<LsmOptions>
    ) -> Result<Arc<SSTable>, ()> {
        let sst_file = LsmFile::open(path)?;
        let sst_bytes = sst_file.read_all()?;

        Self::decode(&sst_bytes, id, lsm_options, sst_file)
    }

    fn decode(
        bytes: &Vec<u8>,
        id: usize,
        lsm_options: Arc<LsmOptions>,
        file: LsmFile,
    ) -> Result<Arc<SSTable>, ()> {
        let meta_offset = utils::u8_vec_to_u32_le(bytes, bytes.len() - 4);
        let bloom_offset = utils::u8_vec_to_u32_le(bytes, bytes.len() - 8);
        let level = utils::u8_vec_to_u32_le(bytes, bytes.len() - 12);
        let state = bytes[bytes.len() - 13];

        let block_metadata = BlockMetadata::decode_all(bytes, meta_offset as usize)?;
        let bloom_filter = BloomFilter::decode(bytes, bloom_offset as usize)?;

        let first_key = Self::get_first_key(&block_metadata);
        let last_key = Self::get_last_key(&block_metadata);

        Ok(Arc::new(SSTable::new(
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

    fn get_last_key(block_metadata: &Vec<BlockMetadata>) -> Key {
        block_metadata.get(block_metadata.len() - 1).unwrap().last_key.clone()
    }

    fn get_first_key(block_metadata: &Vec<BlockMetadata>) -> Key {
        block_metadata.get(0).unwrap().first_key.clone()
    }

    pub fn delete(&self) {
        self.state.store(SSTABLE_DELETED, Release);
    }

    pub fn size(&self) -> usize {
        self.file.size()
    }

    pub fn load_block(&self, block_id: usize) -> Result<Arc<Block>, ()> {
        {
            //Try read from cache
            let mut block_cache = self.block_cache.lock()
                .unwrap();
            let block_entry_from_cache = block_cache.get(block_id);

            if block_entry_from_cache.is_some() {
                return Ok::<Arc<Block>, ()>(block_entry_from_cache.unwrap());
            }
        }

        //Read from disk
        let metadata = &self.block_metadata[block_id];
        let encoded_block = self.file.read(metadata.offset, self.lsm_options.block_size_bytes)?;
        let block = Block::decode(&encoded_block, &self.lsm_options)?;
        let block = Arc::new(block);

        {
            //Write to cache
            let mut block_cache = self.block_cache.lock()
                .unwrap();
            block_cache.put(block_id, block.clone());
        }

        Ok(block)
    }

    pub fn get(&self, key: &Key) -> Option<bytes::Bytes> {
        if self.first_key.lt(key) && self.last_key.gt(key) {
            return None;
        }
        if !self.bloom_filter.may_contain(utils::hash(key.as_bytes())) {
            return None;
        }

        return match self.get_block_metadata(key) {
            Some((index, _)) => {
                self.load_block(index)
                    .expect("Error whiile reading block")
                    .get_value(key)
            },
            None => None
        };
    }

    pub(crate) fn get_block_metadata(&self, key: &Key) -> Option<(usize, &BlockMetadata)> {
        let mut right = self.block_metadata.len() - 1;
        let mut left = 0;

        loop {
            let current_index = (left + right) / 2;
            let current_block_metadata = &self.block_metadata[current_index];

            if left == right {
                return None;
            }
            if current_block_metadata.contains(key) {
                return Some((current_index, current_block_metadata));
            }
            if current_block_metadata.first_key.gt(key) {
                right = current_index;
            }
            if current_block_metadata.last_key.lt(key) {
                left = current_index;
            }
        }
    }
}

pub struct BlockMetadata {
    pub(crate) offset: usize,
    pub(crate) first_key: Key,
    pub(crate) last_key: Key
}

impl BlockMetadata {
    pub fn decode_all(bytes: &Vec<u8>, start_index: usize) -> Result<Vec<BlockMetadata>, ()> {
        let expected_crc = utils::u8_vec_to_u32_le(bytes, start_index);
        let n_blocks_metadata = utils::u8_vec_to_u32_le(bytes, start_index + 4);

        let mut last_index: usize = start_index;
        let mut blocks_metadata_decoded: Vec<BlockMetadata> = Vec::with_capacity(n_blocks_metadata as usize);
        for _ in 0..n_blocks_metadata {
            let (new_last_index, blockmetadata_decoded) = Self::decode(&bytes, last_index)?;
            last_index = new_last_index;
            blocks_metadata_decoded.push(blockmetadata_decoded);
        }

        let actual_crc = crc32fast::hash(&bytes[start_index..last_index]);
        if actual_crc != expected_crc {
            return Err(());
        }

        Ok((blocks_metadata_decoded))
    }

    pub fn encode_all(blocks_metadata: &Vec<BlockMetadata>) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::new();

        let mut metadata_encoded: Vec<u8> = Vec::new();
        for block_metadata in blocks_metadata {
            metadata_encoded.extend(block_metadata.encode());
        }

        encoded.put_u32_le(crc32fast::hash(&metadata_encoded));
        encoded.put_u32_le(blocks_metadata.len() as u32);
        encoded.extend(metadata_encoded);
        encoded
    }

    pub fn decode(bytes: &Vec<u8>, start_index: usize) -> Result<(usize, BlockMetadata), ()> {
        let mut current_index = start_index;

        let first_key_length = utils::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;
        let first_key = String::from_utf8(bytes[current_index..first_key_length].to_vec())
            .expect("Cannot parse utf8");
        current_index = current_index + (first_key_length + 4);

        let last_key_length = utils::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;
        let last_key = String::from_utf8(bytes[current_index..last_key_length].to_vec())
            .expect("Cannot parse utf8");
        current_index = current_index + (last_key_length + 4) as usize;

        let offset = utils::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;

        Ok((current_index, BlockMetadata{
            first_key: key::new(first_key.as_str()),
            last_key: key::new(last_key.as_str()),
            offset
        }))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut metadata_encoded: Vec<u8> = Vec::new();
        metadata_encoded.put_u32_le(self.first_key.len() as u32);
        metadata_encoded.extend(self.first_key.as_bytes());
        metadata_encoded.put_u32_le(self.last_key.len() as u32);
        metadata_encoded.extend(self.last_key.as_bytes());
        metadata_encoded.put_u32_le(self.offset as u32);
        metadata_encoded
    }

    pub fn contains(&self, key: &Key) -> bool {
        self.first_key.le(key) && self.last_key.ge(key)
    }
}

impl Clone for BlockMetadata {
    fn clone(&self) -> Self {
        BlockMetadata{
            offset: self.offset,
            first_key: self.first_key.clone(),
            last_key: self.last_key.clone(),
        }
    }
}