use std::path::Path;
use std::sync::Arc;
use bytes::{BufMut, Bytes};
use crate::sst::block::block_builder::BlockBuilder;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::sst::block_metadata::BlockMetadata;
use crate::sst::sstable::{SSTable, SSTABLE_ACTIVE};
use crate::utils::bloom_filter::BloomFilter;
use crate::utils::lsm_file::{LsmFile, LsmFileMode};
use crate::utils::utils;

pub struct SSTableBuilder {
    first_key: Option<Key>,
    last_key: Option<Key>,

    current_block_builder: BlockBuilder,
    first_key_current_block: Option<Key>,
    last_key_current_block: Option<Key>,

    builded_block_metadata: Vec<BlockMetadata>,
    builded_encoded_blocks: Vec<u8>,

    key_hashes: Vec<u32>,

    lsm_options: Arc<LsmOptions>,
    level: u32,

    memtable_id: Option<usize>,
}

impl SSTableBuilder {
    pub fn new(lsm_options: Arc<LsmOptions>, level: u32) -> SSTableBuilder {
        SSTableBuilder {
            current_block_builder: BlockBuilder::new(lsm_options.clone()),
            level,
            key_hashes: Vec::new(),
            builded_block_metadata: Vec::new(),
            builded_encoded_blocks: Vec::new(),
            first_key_current_block: None,
            last_key_current_block: None,
            memtable_id: None,
            first_key: None,
            last_key: None,
            lsm_options,
        }
    }

    pub fn set_memtable_id(&mut self, memtable_id: usize) {
        self.memtable_id = Some(memtable_id);
    }

    pub fn add_entry(&mut self, key: Key, value: Bytes) {
        if self.first_key.is_none() {
            self.first_key = Some(key.clone());
        }
        self.last_key = Some(key.clone());

        self.last_key_current_block = Some(key.clone());
        if self.first_key_current_block.is_none() {
            self.first_key_current_block = Some(key.clone());
        }

        self.key_hashes.push(utils::hash(key.as_bytes()));

        match self.current_block_builder.add_entry(key, value) {
            Err(_) => self.build_current_block(),
            Ok(_) => {}
        };
    }

    pub fn n_entries(&self) -> usize {
        self.key_hashes.len()
    }

    pub fn get_memtable_id(&self) -> Option<usize> {
        self.memtable_id
    }

    pub fn build(
        mut self,
        id: usize,
        path: &Path
    ) -> Result<SSTable, ()> {
        self.build_current_block();

        let bloom_filter: BloomFilter = BloomFilter::new(
            &self.key_hashes,
            self.lsm_options.bloom_filter_n_entries
        );

        let mut encoded = self.builded_encoded_blocks;

        //Blocks metadata
        let meta_offset = encoded.len();
        encoded.extend(BlockMetadata::encode_all(&self.builded_block_metadata));
        //Block
        let bloom_offset = encoded.len();
        encoded.extend(bloom_filter.encode());
        //Bloom & blocks metadata offsets, state
        encoded.push(SSTABLE_ACTIVE);
        encoded.put_u32_le(self.level);
        encoded.put_u32_le(bloom_offset as u32);
        encoded.put_u32_le(meta_offset as u32);

        match LsmFile::create(path, &encoded, LsmFileMode::ReadOnly) {
            Ok(lsm_file) => Ok(SSTable::new(
                self.builded_block_metadata, self.lsm_options, bloom_filter, self.first_key.unwrap(),
                self.last_key.unwrap(), lsm_file, self.level, id, SSTABLE_ACTIVE
            )),
            Err(_) => Err(())
        }
    }

    pub fn estimated_size_bytes(&self) -> usize {
        self.builded_encoded_blocks.len() + self.lsm_options.block_size_bytes
    }

    fn build_current_block(&mut self) {
        //Nothing to build
        if self.first_key_current_block.is_none() {
            return
        }

        let encoded_block: Vec<u8> = self.current_block_builder.build()
            .encode(&self.lsm_options);
        self.current_block_builder = BlockBuilder::new(self.lsm_options.clone());

        self.builded_block_metadata.push(BlockMetadata {
            first_key: self.first_key_current_block.take().unwrap(),
            last_key: self.last_key_current_block.take().unwrap(),
            offset: self.builded_encoded_blocks.len(),
        });

        let crc = crc32fast::hash(&encoded_block);
        self.builded_encoded_blocks.extend(encoded_block);
        self.builded_encoded_blocks.put_u32_le(crc);
    }
}