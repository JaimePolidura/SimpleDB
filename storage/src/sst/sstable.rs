use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
use crate::sst::block::blocks::Blocks;
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
use std::sync::Arc;

pub const SSTABLE_DELETED: u8 = 2;
pub const SSTABLE_ACTIVE: u8 = 1;

pub struct SSTable {
    pub(crate) sstable_id: shared::SSTableId,
    pub(crate) bloom_filter: BloomFilter,
    pub(crate) file: SimpleDbFileWrapper,
    pub(crate) blocks: Blocks,
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
            blocks: Blocks::create(
                keyspace_desc, block_metadata, options.clone(),
                SimpleDbFileWrapper {file: UnsafeCell::new(file.clone())},
                sstable_id
            ),
            file: SimpleDbFileWrapper {file: UnsafeCell::new(file)},
            state: AtomicU8::new(state),
            keyspace_desc,
            bloom_filter,
            sstable_id,
            first_key,
            last_key,
            level,
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

        let block_metadata = BlockMetadata::deserialize_all(bytes, meta_offset as usize, keyspace_desc.key_type)
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
    
    pub fn get(&self, key: &Bytes, transaction: &Transaction) -> Result<Option<bytes::Bytes>, shared::SimpleDbError> {
        if self.first_key.bytes_gt_bytes(key) || self.last_key.bytes_lt_bytes(key) {
            return Ok(None);
        }
        if !self.bloom_filter.may_contain(shared::hash(key.as_ref())) {
            return Ok(None);
        }

        self.blocks.get(key, transaction)
    }
}