use std::path::Path;
use std::sync::Arc;
use bytes::{BufMut, Bytes};
use crate::block::block_builder::BlockBuilder;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::sst::sstable::{BlockMetadata, SSTable};
use crate::utils::bloom_filter::BloomFilter;
use crate::utils::lsm_file::LsmFile;
use crate::utils::utils;

pub struct SSTableBuilder {
    current_block_builder: BlockBuilder,

    block_metadata: Vec<BlockMetadata>,
    encoded_blocks: Vec<u8>,

    key_hashes: Vec<u32>,

    first_key: Option<Key>,
    last_key: Option<Key>,

    lsm_options: Arc<LsmOptions>
}

impl SSTableBuilder {
    pub fn new(lsm_options: Arc<LsmOptions>) -> SSTableBuilder {
        SSTableBuilder {
            current_block_builder: BlockBuilder::new(lsm_options.clone()),
            key_hashes: Vec::new(),
            block_metadata: Vec::new(),
            encoded_blocks: Vec::new(),
            first_key: None,
            last_key: None,
            lsm_options,
        }
    }

    pub fn add_entry(&mut self, key: Key, value: Bytes) {
        self.last_key = Some(key.clone());
        if self.first_key.is_none() {
            self.first_key = Some(key.clone());
        }

        self.key_hashes.push(utils::hash(key.as_bytes()));

        match self.current_block_builder.add_entry(key, value) {
            Err(_) => self.build_current_block(),
            Ok(_) => {}
        };
    }

    pub fn build (
        mut self,
        id: usize,
        path: &Path
    ) -> Result<SSTable, ()> {
        self.build_current_block();

        let bloom_filter: BloomFilter = BloomFilter::new(
            &self.key_hashes,
            self.lsm_options.bloom_filter_n_entries
        );

        let mut encoded = self.encoded_blocks;

        //Blocks metadata
        let meta_offset = encoded.len();
        encoded.extend(BlockMetadata::encode_all(&self.block_metadata));
        //Block
        let bloom_offset = encoded.len();
        encoded.extend(bloom_filter.encode());
        //Bloom & blocks metadata offsets
        encoded.put_u32_le(bloom_offset as u32);
        encoded.put_u32_le(meta_offset as u32);

        match LsmFile::create(path, &encoded) {
            Ok(lsm_file) => Ok(SSTable::new(
                self.block_metadata, bloom_filter, self.lsm_options, lsm_file, id
            )),
            Err(_) => Err(())
        }
    }

    pub fn estimated_size_bytes(&self) -> usize {
        self.encoded_blocks.len() + self.lsm_options.block_size_bytes
    }

    fn build_current_block(&mut self) {
        let encoded_block: Vec<u8> = self.current_block_builder.build()
            .encode(&self.lsm_options);
        self.current_block_builder = BlockBuilder::new(self.lsm_options.clone());

        self.block_metadata.push(BlockMetadata {
            first_key: self.first_key.take().unwrap(),
            last_key: self.last_key.take().unwrap(),
            offset: self.encoded_blocks.len(),
        });

        let crc = crc32fast::hash(&encoded_block);
        self.encoded_blocks.extend(encoded_block);
        self.encoded_blocks.put_u32_le(crc);
    }
}