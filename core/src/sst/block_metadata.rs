use std::string::FromUtf8Error;
use bytes::BufMut;
use crate::key;
use crate::key::Key;
use crate::lsm_error::{DecodeErrorInfo, DecodeErrorType, LsmError, SSTableCorruptedPart};
use crate::lsm_error::LsmError::CannotDecodeSSTable;
use crate::utils::utils;

#[derive(Eq, PartialEq)]
pub struct BlockMetadata {
    pub(crate) offset: usize,
    pub(crate) first_key: Key,
    pub(crate) last_key: Key
}

impl BlockMetadata {
    pub fn decode_all(
        bytes: &Vec<u8>,
        start_index: usize,
    ) -> Result<Vec<BlockMetadata>, DecodeErrorType> {
        let expected_crc = utils::u8_vec_to_u32_le(bytes, start_index);
        let n_blocks_metadata = utils::u8_vec_to_u32_le(bytes, start_index + 4);

        let mut last_index: usize = start_index + 8;
        let start_content_index = last_index;
        let mut blocks_metadata_decoded: Vec<BlockMetadata> = Vec::with_capacity(n_blocks_metadata as usize);
        for _ in 0..n_blocks_metadata {
            let (new_last_index, blockmetadata_decoded) = Self::decode(&bytes, last_index)?;

            last_index = new_last_index;
            blocks_metadata_decoded.push(blockmetadata_decoded);
        }

        let actual_crc = crc32fast::hash(&bytes[start_content_index..last_index]);
        if actual_crc != expected_crc {
            return Err(DecodeErrorType::CorruptedCrc(expected_crc, actual_crc));
        }

        Ok(blocks_metadata_decoded)
    }

    pub fn encode_all(blocks_metadata: &Vec<BlockMetadata>) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::new();

        let mut metadata_encoded: Vec<u8> = Vec::new();
        for block_metadata in blocks_metadata {
            metadata_encoded.extend(block_metadata.encode());
        }

        encoded.put_u32_le(crc32fast::hash(&metadata_encoded));
        encoded.put_u32_le(blocks_metadata.len() as u32);
        encoded.extend(metadata_encoded);
        encoded
    }

    pub fn decode(bytes: &Vec<u8>, start_index: usize) -> Result<(usize, BlockMetadata), DecodeErrorType> {
        let mut current_index = start_index;

        let first_key_length = utils::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;
        let first_key = String::from_utf8(bytes[current_index..(current_index + first_key_length)].to_vec())
            .map_err(|e| DecodeErrorType::Utf8Decode(e))?;
        current_index = current_index + first_key_length;

        let last_key_length = utils::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;
        let last_key = String::from_utf8(bytes[current_index..(current_index + last_key_length)].to_vec())
            .map_err(|e| DecodeErrorType::Utf8Decode(e))?;

        current_index = current_index + last_key_length;

        let offset = utils::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;

        Ok((current_index, BlockMetadata{
            first_key: key::new(first_key.as_str()),
            last_key: key::new(last_key.as_str()),
            offset
        }))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut metadata_encoded: Vec<u8> = Vec::new();
        metadata_encoded.put_u32_le(self.first_key.len() as u32);
        metadata_encoded.extend(self.first_key.as_bytes());
        metadata_encoded.put_u32_le(self.last_key.len() as u32);
        metadata_encoded.extend(self.last_key.as_bytes());
        metadata_encoded.put_u32_le(self.offset as u32);
        metadata_encoded
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

#[cfg(test)]
mod test {
    use crate::key;
    use crate::sst::block_metadata::BlockMetadata;

    #[test]
    fn encode_decode() {
        let metadata = vec![
            BlockMetadata{offset: 0, first_key: key::new("a"), last_key: key::new("b")},
            BlockMetadata{offset: 1, first_key: key::new("b"), last_key: key::new("c")},
            BlockMetadata{offset: 2, first_key: key::new("c"), last_key: key::new("d")},
            BlockMetadata{offset: 3, first_key: key::new("d"), last_key: key::new("z")},
        ];
        let encoded = BlockMetadata::encode_all(&metadata);
        let decoded = BlockMetadata::decode_all(&encoded, 0, 0);

        assert!(decoded.is_ok());
        let decoded = decoded.unwrap();

        assert_eq!(decoded.len(), 4);

        assert!(decoded[0].offset == 0 && decoded[0].first_key == key::new("a") && decoded[0].last_key == key::new("b"));
        assert!(decoded[1].offset == 1 && decoded[1].first_key == key::new("b") && decoded[1].last_key == key::new("c"));
        assert!(decoded[2].offset == 2 && decoded[2].first_key == key::new("c") && decoded[2].last_key == key::new("d"));
        assert!(decoded[3].offset == 3 && decoded[3].first_key == key::new("d") && decoded[3].last_key == key::new("z"));
    }
}
