use std::sync::Arc;
use bytes::{BufMut, Bytes};
use block::{NOT_COMPRESSED, PREFIX_COMPRESSED};
use shared::key::Key;
use crate::sst::block::block;
use crate::sst::block::block::Block;

pub(crate) fn decode_block(
    encoded: &Vec<u8>,
    options: &Arc<shared::SimpleDbOptions>
) -> Result<Block, shared::DecodeErrorType> {
    if encoded.len() != options.block_size_bytes {
        return Err(shared::DecodeErrorType::IllegalSize(options.block_size_bytes, encoded.len()));
    }

    let flag: u64 = shared::u8_vec_to_u64_le(&encoded, options.block_size_bytes - 12);
    let offsets_offset: u16 = shared::u8_vec_to_u16_le(&encoded, options.block_size_bytes - 2);
    let n_entries: u16 = shared::u8_vec_to_u16_le(&encoded, options.block_size_bytes - 4);
    let offsets = decode_offsets(encoded, offsets_offset, n_entries);
    let (entries, new_offsets) = match flag {
        PREFIX_COMPRESSED => Ok(decode_entries_prefix_compressed(encoded, &offsets)),
        NOT_COMPRESSED => Ok((decode_entries_not_compressed(encoded, offsets_offset), offsets)),
        _ => Err(shared::DecodeErrorType::UnknownFlag(flag as usize)),
    }?;

    Ok(Block{ offsets: new_offsets, entries })
}

fn decode_offsets(
    encoded: &Vec<u8>,
    offsets_offset: u16,
    n_entries: u16,
) -> Vec<u16> {
    let start_index = offsets_offset as usize;
    let end_index = start_index + (n_entries * std::mem::size_of::<u16>() as u16) as usize;

    shared::u8_vec_to_u16_vec(&encoded[start_index..end_index].to_vec())
}

fn decode_entries_prefix_compressed(
    encoded: &Vec<u8>,
    offsets: &Vec<u16>,
) -> (Vec<u8>, Vec<u16>) {
    let mut entries_decoded: Vec<u8> = Vec::new();
    let mut prev_key: Option<Key> = None;
    let mut new_offsets: Vec<u16> = Vec::new();

    for current_offset in offsets {
        //Decode key
        let mut current_index: usize = *current_offset as usize;
        new_offsets.push(entries_decoded.len() as u16);

        let key_overlap_size = shared::u8_vec_to_u16_le(encoded, current_index);
        current_index = current_index + 2;
        let rest_key_size = shared::u8_vec_to_u16_le(encoded, current_index);
        current_index = current_index + 2;
        let key_txn_id = shared::u8_vec_to_u64_le(encoded, current_index) as shared::TxnId;
        current_index = current_index + 8;
        let rest_key_u8_vec = encoded[current_index..(current_index + rest_key_size as usize)].to_vec();

        let current_key = match prev_key.as_ref() {
            Some(prev_key) => {
                let (overlaps, _) = prev_key.split(key_overlap_size as usize);
                let rest_key = Key::create(Bytes::from(rest_key_u8_vec), key_txn_id);
                Key::merge(&overlaps, &rest_key, key_txn_id)
            },
            None => Key::create(Bytes::from(rest_key_u8_vec), key_txn_id)
        };
        current_index = current_index + rest_key_size as usize;
        entries_decoded.extend(current_key.serialize());
        prev_key = Some(current_key);

        //Decode value
        let value_size = shared::u8_vec_to_u16_le(encoded, current_index);
        current_index = current_index + 2;
        let value = &encoded[current_index..(current_index + value_size as usize)];
        entries_decoded.put_u16_le(value_size);
        entries_decoded.extend(value);
    }

    (entries_decoded, new_offsets)
}

fn decode_entries_not_compressed(
    encoded: &Vec<u8>,
    offsets_offset: u16,
) -> Vec<u8> {
    let start_index = 0;
    let end_index = offsets_offset as usize;

    encoded[start_index..=end_index].to_vec()
}