use crate::sst::block::block_builder::BlockBuilder;
use crate::sst::block_metadata::BlockMetadata;
use crate::sst::sstable::{SSTable, SSTABLE_ACTIVE};
use crate::utils::bloom_filter::BloomFilter;
use bytes::{BufMut, Bytes};
use shared::key::Key;
use std::path::Path;
use std::sync::Arc;

pub struct SSTableBuilder {
    first_key: Option<Key>,
    last_key: Option<Key>,

    current_block_builder: BlockBuilder,
    first_key_current_block: Option<Key>,
    last_key_current_block: Option<Key>,

    builded_block_metadata: Vec<BlockMetadata>,
    builded_encoded_blocks: Vec<u8>,

    key_hashes: Vec<u32>,

    options: Arc<shared::SimpleDbOptions>,
    level: u32,

    memtable_id: Option<usize>,

    keyspace_id: shared::KeyspaceId,
}

impl SSTableBuilder {
    pub fn create(
        options: Arc<shared::SimpleDbOptions>,
        keyspace_id: shared::KeyspaceId,
        level: u32
    ) -> SSTableBuilder {
        SSTableBuilder {
            current_block_builder: BlockBuilder::create(options.clone()),
            level,
            keyspace_id,
            key_hashes: Vec::new(),
            builded_block_metadata: Vec::new(),
            builded_encoded_blocks: Vec::new(),
            first_key_current_block: None,
            last_key_current_block: None,
            memtable_id: None,
            first_key: None,
            last_key: None,
            options,
        }
    }

    pub fn is_from_memtable(&self) -> bool {
        self.memtable_id.is_some()
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

        self.key_hashes.push(shared::hash(key.as_bytes()));

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
    ) -> Result<SSTable, shared::SimpleDbError> {
        self.build_current_block();

        let bloom_filter: BloomFilter = BloomFilter::create(
            &self.key_hashes,
            self.options.bloom_filter_n_entries
        );

        let mut encoded = self.builded_encoded_blocks;

        //Blocks metadata
        let meta_offset = encoded.len();
        let meta_encoded = BlockMetadata::encode_all(&self.builded_block_metadata);
        encoded.extend(meta_encoded);

        //Bloom
        let bloom_offset = encoded.len();
        let bloom_encoded = bloom_filter.encode();
        encoded.extend(bloom_encoded);

        //Bloom & blocks metadata offsets, state
        encoded.push(SSTABLE_ACTIVE);
        encoded.put_u32_le(self.level);
        encoded.put_u32_le(bloom_offset as u32);
        encoded.put_u32_le(meta_offset as u32);

        match shared::SimpleDbFile::create(path, &encoded, shared::SimpleDbFileMode::ReadOnly) {
            Ok(lsm_file) => Ok(SSTable::create(self.builded_block_metadata, self.options, bloom_filter, self.first_key.unwrap(),
                                               self.last_key.unwrap(), lsm_file, self.level, id, SSTABLE_ACTIVE, self.keyspace_id,
            )),
            Err(e) => Err(shared::SimpleDbError::   CannotCreateSSTableFile(self.keyspace_id, id, e))
        }
    }

    pub fn estimated_size_bytes(&self) -> usize {
        self.builded_encoded_blocks.len() + self.options.block_size_bytes
    }

    fn build_current_block(&mut self) {
        //Nothing to build
        if self.first_key_current_block.is_none() {
            return
        }

        let encoded_block: Vec<u8> = self.current_block_builder.build()
            .serialize(&self.options);
        self.current_block_builder = BlockBuilder::create(self.options.clone());

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