use crate::sst::block::block::{Block, BLOCK_FOOTER_LENGTH, NOT_COMPRESSED, PREFIX_COMPRESSED};
use bytes::BufMut;
use std::sync::Arc;
use shared::key::Key;

impl Block {
    pub fn serialize(&self, options: &Arc<shared::SimpleDbOptions>) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::with_capacity(options.block_size_bytes);

        let (prefix_compression_succeeded, offsets_updated) = self.serialize_entries(&mut serialized, PREFIX_COMPRESSED, options);
        if prefix_compression_succeeded {
            let start_offsets_offset = self.serialize_offsets(&offsets_updated, &mut serialized);
            self.serialize_footer(start_offsets_offset, &mut serialized, PREFIX_COMPRESSED, options);
        } else {
            serialized.clear();
            self.serialize_entries(&mut serialized, NOT_COMPRESSED, options);
            let start_offsets_offset = self.serialize_offsets(&self.offsets, &mut serialized);
            self.serialize_footer(start_offsets_offset, &mut serialized, NOT_COMPRESSED, options);
        }

        serialized
    }

    //Success if prefix compressed was successful & new offsets
    fn serialize_entries(
        &self,
        serialized: &mut Vec<u8>,
        flags: u64,
        options: &Arc<shared::SimpleDbOptions>
    ) -> (bool, Vec<u16>) {
        match flags {
            PREFIX_COMPRESSED => self.serialize_prefix_compressed_entries(serialized, options),
            NOT_COMPRESSED => { serialized.extend(&self.entries); (true, self.offsets.to_vec()) },
            _ => panic!("Illegal block flags when encoding")
        }
    }

    fn serialize_prefix_compressed_entries(
        &self,
        serialized: &mut Vec<u8>,
        options: &Arc<shared::SimpleDbOptions>
    ) -> (bool, Vec<u16>) {
        let mut prev_key: Option<Key> = None;
        let mut new_offsets: Vec<u16> = Vec::new();
        let mut current_size: usize = BLOCK_FOOTER_LENGTH + (self.offsets.len() * std::mem::size_of::<u16>());

        for current_index in 0..self.offsets.len() {
            let (current_value, _) = self.get_value_by_index(current_index);
            let current_key = self.get_key_by_index(current_index);
            new_offsets.push(serialized.len() as u16);

            match prev_key {
                Some(prev_key) => {
                    //Key
                    let (key_overlap_size, rest_key_size) = current_key.prefix_difference(&prev_key);
                    let (_, rest_key) = current_key.split(key_overlap_size);
                    serialized.put_u16_le(key_overlap_size as u16);
                    serialized.put_u16_le(rest_key_size as u16);
                    serialized.put_u64_le(current_key.txn_id() as u64);
                    if !rest_key.is_empty() {
                        serialized.extend(rest_key.as_bytes());
                    }
                    //Value
                    serialized.put_u16_le(current_value.len() as u16);
                    serialized.extend(current_value.as_ref());
                },
                None => {
                    //Key
                    serialized.put_u16_le(0);
                    serialized.put_u16_le(current_key.len() as u16);
                    serialized.put_u64_le(current_key.txn_id() as u64);
                    serialized.extend(current_key.as_bytes());
                    //Value
                    serialized.put_u16_le(current_value.len() as u16);
                    serialized.extend(current_value.as_ref());
                },
            }

            current_size = current_size + serialized.len();
            prev_key = Some(current_key);
        }

        (current_size < options.block_size_bytes, new_offsets)
    }

    fn serialize_offsets(&self, offsets: &Vec<u16>, serialized: &mut Vec<u8>) -> usize {
        let offsets_offset_xd = serialized.len();
        serialized.extend(shared::u16_vec_to_u8_vec(offsets));
        offsets_offset_xd
    }

    fn serialize_footer(
        &self,
        start_offsets_offset: usize,
        serialized: &mut Vec<u8>,
        flags: u64,
        options: &Arc<shared::SimpleDbOptions>
    ) {
        let n_entries: u16 = self.offsets.len() as u16;
        shared::u64_to_u8_le(flags, options.block_size_bytes - 12, serialized);
        shared::u16_to_u8_le(n_entries, options.block_size_bytes - 4, serialized);
        shared::u16_to_u8_le(start_offsets_offset as u16, options.block_size_bytes - 2, serialized);
    }
}