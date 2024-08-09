use std::sync::Arc;
use bytes::BufMut;
use crate::block::block::{Block, BLOCK_FOOTER_LENGTH, NOT_COMPRESSED, PREFIX_COMPRESSED};
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::utils::utils;

pub(crate) fn encode_block(
    block: &Block,
    options: &Arc<LsmOptions>
) -> Vec<u8> {
    let mut encoded: Vec<u8> = Vec::with_capacity(options.block_size_bytes);

    let (prefix_compression_succeeded, offsets_updated) = encode_entries(block, &mut encoded, PREFIX_COMPRESSED, options);
    if prefix_compression_succeeded {
        let start_offsets_offset = encode_offsets(&offsets_updated, &mut encoded);
        encode_footer(start_offsets_offset, block, &mut encoded, PREFIX_COMPRESSED, options);
    } else {
        encode_entries(block, &mut encoded, NOT_COMPRESSED, options);
        let start_offsets_offset = encode_offsets(&block.offsets, &mut encoded);
        encode_footer(start_offsets_offset, block, &mut encoded, NOT_COMPRESSED, options);
    }

    encoded
}

//Sucess if prefix compressed was sucessful & new offsets
fn encode_entries(
    block: &Block,
    encoded: &mut Vec<u8>,
    flags: u64,
    options: &Arc<LsmOptions>
) -> (bool, Vec<u16>) {
    match flags {
        PREFIX_COMPRESSED => encode_prefix_compressed_entries(block, encoded, options),
        NOT_COMPRESSED => { encoded.extend(&block.entries); (true, block.offsets.to_vec()) },
        _ => panic!("Illegal block flags when encoding")
    }
}

fn encode_prefix_compressed_entries(
    block: &Block,
    encoded: &mut Vec<u8>,
    options: &Arc<LsmOptions>
) -> (bool, Vec<u16>) {
    let mut prev_key: Option<Key> = None;
    let mut new_offsets: Vec<u16> = Vec::new();
    let mut current_size: usize = BLOCK_FOOTER_LENGTH + (block.offsets.len() * std::mem::size_of::<u16>());

    for current_index in 0..block.offsets.len() {
        let current_value = block.get_value_by_index(current_index);
        let current_key = block.get_key_by_index(current_index);
        new_offsets.push(encoded.len() as u16);

        match prev_key {
            Some(prev_key) => {
                //Key
                let (key_overlap_size, rest_key_size) = current_key.prefix_difference(&prev_key);
                let (_, rest_key) = current_key.split(key_overlap_size);
                encoded.put_u16_le(key_overlap_size as u16);
                encoded.put_u16_le(rest_key_size as u16);
                if !rest_key.is_empty() {
                    encoded.extend(rest_key.as_bytes());
                }
                //Value
                encoded.put_u16_le(current_value.len() as u16);
                encoded.put_slice(current_value.as_ref());
            },
            None => {
                //Key
                encoded.put_u16_le(0);
                encoded.put_u16_le(current_key.len() as u16);
                encoded.extend(current_key.as_bytes());
                //Value
                encoded.put_u16_le(current_value.len() as u16);
                encoded.put_slice(current_value.as_ref());
            },
        }

        current_size = current_size + encoded.len();
        prev_key = Some(current_key);
    }

    (current_size < options.block_size_bytes, new_offsets)
}

fn encode_offsets(offsets: &Vec<u16>, encoded: &mut Vec<u8>) -> usize {
    let offsets_offset_xd = encoded.len();
    encoded.extend(utils::u16_vec_to_u8_vec(offsets));
    offsets_offset_xd
}

fn encode_footer(
    start_offsets_offset: usize,
    block: &Block,
    encoded: &mut Vec<u8>,
    flags: u64,
    options: &Arc<LsmOptions>
) {
    let n_entries: u16 = block.offsets.len() as u16;
    utils::u64_to_u8_le(flags, options.block_size_bytes - 12, encoded);
    utils::u16_to_u8_le(n_entries, options.block_size_bytes - 4, encoded);
    utils::u16_to_u8_le(start_offsets_offset as u16, options.block_size_bytes - 2, encoded);
}