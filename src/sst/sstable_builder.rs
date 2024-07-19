use std::path::Path;
use bytes::{BufMut, Bytes};
use crate::block::block_builder::BlockBuilder;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::sst::sstable::{BlockMetadata, SSTable};
use crate::utils::lsm_file::LSMFile;

pub struct SSTableBuilder {
    current_block_builder: BlockBuilder,

    block_metadata: Vec<BlockMetadata>,
    encoded_blocks: Vec<u8>,

    first_key: Option<Key>,
    last_key: Option<Key>,

    lsm_options: LsmOptions
}

impl SSTableBuilder {
    pub fn new(lsm_options: LsmOptions) -> SSTableBuilder {
        SSTableBuilder {
            current_block_builder: BlockBuilder::new(lsm_options),
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

        let mut encoded = self.encoded_blocks;
        let meta_offset = encoded.len();
        encoded.extend(BlockMetadata::encode(&self.block_metadata));
        encoded.put_u32_le(meta_offset as u32);

        let lsm_file = LSMFile::create(path, &encoded)?;

        Ok(SSTable::new(lsm_file, id))
    }

    pub fn estimated_size_bytes(&self) -> usize {
        self.encoded_blocks.len() + self.lsm_options.block_size_bytes;
    }

    fn build_current_block(&mut self) {
        let encoded_block: Vec<u8> = self.current_block_builder.build()
            .encode(self.lsm_options);
        self.current_block_builder = BlockBuilder::new(self.lsm_options);

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