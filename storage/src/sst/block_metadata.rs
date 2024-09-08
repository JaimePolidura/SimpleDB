use crate::key;
use crate::key::Key;
use bytes::BufMut;
use crate::transactions::transaction::{Transaction};

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
    ) -> Result<Vec<BlockMetadata>, shared::DecodeErrorType> {
        let expected_crc = shared::u8_vec_to_u32_le(bytes, start_index);
        let n_blocks_metadata = shared::u8_vec_to_u32_le(bytes, start_index + 4);

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
            return Err(shared::DecodeErrorType::CorruptedCrc(expected_crc, actual_crc));
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

    pub fn decode(bytes: &Vec<u8>, start_index: usize) -> Result<(usize, BlockMetadata), shared::DecodeErrorType> {
        let mut current_index = start_index;

        let first_key_length = shared::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;
        let first_key_txn_id = shared::u8_vec_to_u64_le(&bytes, current_index) as shared::TxnId;
        current_index = current_index + 8;
        let first_key = String::from_utf8(bytes[current_index..(current_index + first_key_length)].to_vec())
            .map_err(|e| shared::DecodeErrorType::Utf8Decode(e))?;
        current_index = current_index + first_key_length;

        let last_key_length = shared::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;
        let last_key_txn_id = shared::u8_vec_to_u64_le(&bytes, current_index) as shared::TxnId;
        current_index = current_index + 8;
        let last_key = String::from_utf8(bytes[current_index..(current_index + last_key_length)].to_vec())
            .map_err(|e| shared::DecodeErrorType::Utf8Decode(e))?;

        current_index = current_index + last_key_length;

        let offset = shared::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;

        Ok((current_index, BlockMetadata{
            first_key: key::new(first_key.as_str(), first_key_txn_id),
            last_key: key::new(last_key.as_str(), last_key_txn_id),
            offset
        }))
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut metadata_encoded: Vec<u8> = Vec::new();
        //First key
        metadata_encoded.put_u32_le(self.first_key.len() as u32);
        metadata_encoded.put_u64_le(self.first_key.txn_id() as u64);
        metadata_encoded.extend(self.first_key.as_bytes());

        //Las key
        metadata_encoded.put_u32_le(self.last_key.len() as u32);
        metadata_encoded.put_u64_le(self.last_key.txn_id() as u64);
        metadata_encoded.extend(self.last_key.as_bytes());
        metadata_encoded.put_u32_le(self.offset as u32);
        metadata_encoded
    }

    pub fn contains_key_str(&self, key: &str) -> bool {
        self.first_key.as_str().le(key) && self.last_key.as_str().ge(key)
    }

    pub fn contains(&self, key: &str, transaction: &Transaction) -> bool {
        let key_to_be_checked = key::new(key, transaction.txn_id);
        self.first_key.le(&key_to_be_checked) && self.last_key.gt(&key_to_be_checked)
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
            BlockMetadata{offset: 0, first_key: key::new("a", 1), last_key: key::new("b", 1)},
            BlockMetadata{offset: 1, first_key: key::new("b", 1), last_key: key::new("c", 1)},
            BlockMetadata{offset: 2, first_key: key::new("c", 1), last_key: key::new("d", 1)},
            BlockMetadata{offset: 3, first_key: key::new("d", 1), last_key: key::new("z", 1)},
        ];
        let encoded = BlockMetadata::encode_all(&metadata);
        let decoded = BlockMetadata::decode_all(&encoded, 0);

        assert!(decoded.is_ok());
        let decoded = decoded.unwrap();

        assert_eq!(decoded.len(), 4);

        assert!(decoded[0].offset == 0 && decoded[0].first_key == key::new("a", 1) && decoded[0].last_key == key::new("b", 1));
        assert!(decoded[1].offset == 1 && decoded[1].first_key == key::new("b", 1) && decoded[1].last_key == key::new("c", 1));
        assert!(decoded[2].offset == 2 && decoded[2].first_key == key::new("c", 1) && decoded[2].last_key == key::new("d", 1));
        assert!(decoded[3].offset == 3 && decoded[3].first_key == key::new("d", 1) && decoded[3].last_key == key::new("z", 1));
    }
}
