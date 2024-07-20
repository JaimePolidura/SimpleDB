use bytes::BufMut;
use crate::key::Key;
use crate::utils::bloom_filter::BloomFilter;
use crate::utils::lsm_file::LSMFile;

pub struct SSTable {
    bloom_filter: BloomFilter,
    file: LSMFile,
    id: usize,

    block_metadata: Vec<BlockMetadata>
}

impl SSTable {
    pub fn new(
        block_metadata: Vec<BlockMetadata>,
        bloom_filter: BloomFilter,
        file: LSMFile,
        id: usize
    ) -> SSTable {
        SSTable{ block_metadata, bloom_filter, file, id }
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
}