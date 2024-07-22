use std::sync::{Arc, Mutex};
use bytes::BufMut;
use crate::block::block::Block;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::sst::block_cache::BlockCache;
use crate::utils::bloom_filter::BloomFilter;
use crate::utils::lsm_file::LSMFile;

pub struct SSTable {
    pub(crate) id: usize,
    pub(crate) bloom_filter: BloomFilter,
    pub(crate) file: LSMFile,
    pub(crate) block_cache: Mutex<BlockCache>,
    pub(crate) block_metadata: Vec<BlockMetadata>,
    pub(crate) lsm_options: LsmOptions,
}

impl SSTable {
    pub fn new(
        block_metadata: Vec<BlockMetadata>,
        bloom_filter: BloomFilter,
        lsm_options: LsmOptions,
        file: LSMFile,
        id: usize
    ) -> SSTable {
        SSTable {
            block_cache: Mutex::new(BlockCache::new(lsm_options)),
            block_metadata,
            bloom_filter,
            file,
            id,
            lsm_options
        }
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
        let block = Block::decode(&encoded_block, self.lsm_options)?;
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

pub struct BlockMetadata {
    pub(crate) offset: usize,
    pub(crate) first_key: Key,
    pub(crate) last_key: Key
}

impl BlockMetadata {
    pub fn encode(blocks_metadata: &Vec<BlockMetadata>) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::new();

        for block_metadata in blocks_metadata {
            encoded.put_u32_le(block_metadata.first_key.len() as u32);
            encoded.extend(block_metadata.first_key.as_bytes());
            encoded.put_u32_le(block_metadata.last_key.len() as u32);
            encoded.extend(block_metadata.last_key.as_bytes());
            encoded.put_u32_le(block_metadata.offset as u32);
        }

        encoded.put_u32_le(crc32fast::hash(encoded.as_ref()));

        encoded
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