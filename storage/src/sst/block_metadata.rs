use bytes::{BufMut, Bytes};
use shared::key::Key;
use shared::Type;
use crate::transactions::transaction::{Transaction};

#[derive(Eq, PartialEq)]
pub struct BlockMetadata {
    pub(crate) offset: usize,
    pub(crate) first_key: Key,
    pub(crate) last_key: Key
}

impl BlockMetadata {
    pub fn deserialize_all(
        serialized: &Vec<u8>,
        start_index: usize,
        key_type: Type
    ) -> Result<Vec<BlockMetadata>, shared::DecodeErrorType> {
        let expected_crc = shared::u8_vec_to_u32_le(serialized, start_index);
        let n_blocks_metadata = shared::u8_vec_to_u32_le(serialized, start_index + 4);

        let mut last_index: usize = start_index + 8;
        let start_content_index = last_index;
        let mut blocks_metadata_decoded: Vec<BlockMetadata> = Vec::with_capacity(n_blocks_metadata as usize);
        for _ in 0..n_blocks_metadata {
            let (new_last_index, block_metadata_decoded) = Self::deserialize(&serialized, last_index, key_type)?;

            last_index = new_last_index;
            blocks_metadata_decoded.push(block_metadata_decoded);
        }

        let actual_crc = crc32fast::hash(&serialized[start_content_index..last_index]);
        if actual_crc != expected_crc {
            return Err(shared::DecodeErrorType::CorruptedCrc(expected_crc, actual_crc));
        }

        Ok(blocks_metadata_decoded)
    }

    pub fn serialize_all(blocks_metadata: &Vec<BlockMetadata>) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::new();

        let mut metadata_encoded: Vec<u8> = Vec::new();
        for block_metadata in blocks_metadata {
            metadata_encoded.extend(block_metadata.serialize());
        }

        encoded.put_u32_le(crc32fast::hash(&metadata_encoded));
        encoded.put_u32_le(blocks_metadata.len() as u32);
        encoded.extend(metadata_encoded);
        encoded
    }

    pub fn deserialize(
        bytes: &Vec<u8>,
        start_index: usize,
        key_type: Type
    ) -> Result<(usize, BlockMetadata), shared::DecodeErrorType> {
        let mut current_index = start_index;

        let first_key_length = shared::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;
        let first_key_txn_id = shared::u8_vec_to_u64_le(&bytes, current_index) as shared::TxnId;
        current_index = current_index + 8;
        let first_key = Bytes::from(bytes[current_index..(current_index + first_key_length)].to_vec());
        current_index = current_index + first_key_length;

        let last_key_length = shared::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;
        let last_key_txn_id = shared::u8_vec_to_u64_le(&bytes, current_index) as shared::TxnId;
        current_index = current_index + 8;
        let last_key = Bytes::from(bytes[current_index..(current_index + last_key_length)].to_vec());

        current_index = current_index + last_key_length;

        let offset = shared::u8_vec_to_u32_le(&bytes, current_index) as usize;
        current_index = current_index + 4;

        Ok((current_index, BlockMetadata{
            first_key: Key::create(first_key, key_type, first_key_txn_id),
            last_key: Key::create(last_key, key_type, last_key_txn_id),
            offset
        }))
    }

    pub fn serialize(&self) -> Vec<u8> {
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

    pub fn contains(&self, key: &Bytes, transaction: &Transaction) -> bool {
        let key_to_be_checked = Key::create(key.clone(), self.first_key.get_type(), transaction.txn_id);
        self.first_key.le(&key_to_be_checked) && self.last_key.ge(&key_to_be_checked)
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
    use shared::key::Key;
    use shared::Type;
    use crate::sst::block_metadata::BlockMetadata;

    #[test]
    fn encode_decode() {
        let metadata = vec![
            BlockMetadata{offset: 0, first_key: Key::create_from_str("a", 1), last_key: Key::create_from_str("b", 1)},
            BlockMetadata{offset: 1, first_key: Key::create_from_str("b", 1), last_key: Key::create_from_str("c", 1)},
            BlockMetadata{offset: 2, first_key: Key::create_from_str("c", 1), last_key: Key::create_from_str("d", 1)},
            BlockMetadata{offset: 3, first_key: Key::create_from_str("d", 1), last_key: Key::create_from_str("z", 1)},
        ];
        let encoded = BlockMetadata::serialize_all(&metadata);
        let decoded = BlockMetadata::deserialize_all(&encoded, 0, Type::String);

        assert!(decoded.is_ok());
        let decoded = decoded.unwrap();

        assert_eq!(decoded.len(), 4);

        assert!(decoded[0].offset == 0 && decoded[0].first_key == Key::create_from_str("a", 1) && decoded[0].last_key == Key::create_from_str("b", 1));
        assert!(decoded[1].offset == 1 && decoded[1].first_key == Key::create_from_str("b", 1) && decoded[1].last_key == Key::create_from_str("c", 1));
        assert!(decoded[2].offset == 2 && decoded[2].first_key == Key::create_from_str("c", 1) && decoded[2].last_key == Key::create_from_str("d", 1));
        assert!(decoded[3].offset == 3 && decoded[3].first_key == Key::create_from_str("d", 1) && decoded[3].last_key == Key::create_from_str("z", 1));
    }
}
