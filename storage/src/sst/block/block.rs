use std::sync::Arc;
use bytes::Bytes;
use crate::sst::block::block_decoder::decode_block;
use crate::sst::block::block_encoder::encode_block;
use crate::key;
use crate::key::Key;
use crate::lsm_error::DecodeErrorType;
use crate::lsm_options::LsmOptions;
use crate::transactions::transaction::{Transaction, TxnId};

pub const PREFIX_COMPRESSED: u64 = 0x01;
pub const NOT_COMPRESSED: u64 = 0x00;

pub const BLOCK_FOOTER_LENGTH: usize =
    std::mem::size_of::<u16>() + //Nº Entries
        std::mem::size_of::<u16>() + //Offset entries in the block
        std::mem::size_of::<u64>(); //Flags

pub struct Block {
    pub(crate) entries: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self, options: &Arc<LsmOptions>) -> Vec<u8> {
        encode_block(&self, options)
    }

    pub fn decode(encoded: &Vec<u8>, options: &Arc<LsmOptions>) -> Result<Block, DecodeErrorType> {
        decode_block(encoded, options)
    }

    pub fn get_value(&self, key_lookup: &str, transaction: &Transaction) -> Option<Bytes> {
        let mut right = self.offsets.len() / 2;
        let mut left = 0;

        loop {
            let current_index = (left + right) / 2;
            let mut current_key = self.get_key_by_index(current_index);

            if left == right {
                return None;
            }
            if current_key.as_str().eq(key_lookup) {
                return self.get_value_in_multiple_key_versions(transaction, key_lookup, current_index);
            }
            if current_key.as_str().gt(key_lookup) {
                right = current_index;
            }
            if current_key.as_str().lt(key_lookup) {
                left = current_index;
            }
        }
    }

    //Different versions exists for the same key
    fn get_value_in_multiple_key_versions(
        &self,
        transaction: &Transaction,
        key: &str,
        index: usize
    ) -> Option<Bytes> {
        let mut current_index = index;
        while current_index > 0 && self.get_key_by_index(current_index).as_str().eq(key) {
            current_index = current_index - 1;
        }

        while current_index < self.entries.len() {
            let current_key = self.get_key_by_index(current_index);
            if current_key.as_str().eq(key) {
                return None;
            }
            if transaction.can_read(&current_key) {
                return Some(self.get_value_by_index(current_index));
            }
        }

        None
    }

    //Expect n_entry_index to be an index to block::offsets aray
    pub fn get_key_by_index(&self, n_entry_index: usize) -> Key {
        let entry_index = self.offsets[n_entry_index] as usize;
        let key_length = shared::u8_vec_to_u16_le(&self.entries, entry_index) as usize;
        let key_txn_id = shared::u8_vec_to_u64_le(&self.entries, entry_index + 2) as TxnId;

        let key_slice: &[u8] = &self.entries[entry_index + 10..(key_length + entry_index + 10)];
        let key = String::from_utf8(key_slice.to_vec())
            .expect("Error while parsing with UTF-8");

        key::new(key.as_str(), key_txn_id)
    }

    //Expect n_entry_index to be an index to block::offsets aray
    pub fn get_value_by_index(&self, n_entry_index: usize) -> Bytes {
        let entry_index = self.offsets[n_entry_index];
        let key_length = shared::u8_vec_to_u16_le(&self.entries, entry_index as usize);
        //10 = (key bytes size u16) + (key txn_id length u64)
        let value_index = (entry_index as usize) + 10 + key_length as usize;
        let value_length = shared::u8_vec_to_u16_le(&self.entries, value_index) as usize;

        Bytes::copy_from_slice(&self.entries[(value_index + 2)..((value_index + 2) + value_length)])
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use bytes::Bytes;
    use crate::sst::block::block::Block;
    use crate::sst::block::block_builder::BlockBuilder;
    use crate::key;
    use crate::lsm_options::LsmOptions;

    #[test]
    fn encode_and_decode() {
        let mut block_builder = BlockBuilder::new(Arc::new(LsmOptions::default()));
        block_builder.add_entry(key::new("Jaime", 1), Bytes::from(vec![1]));
        block_builder.add_entry(key::new("Javier", 1), Bytes::from(vec![2]));
        block_builder.add_entry(key::new("Jose", 1), Bytes::from(vec![3]));
        block_builder.add_entry(key::new("Juan", 1), Bytes::from(vec![4]));
        block_builder.add_entry(key::new("Justo", 1), Bytes::from(vec![5]));
        block_builder.add_entry(key::new("Justoo", 1), Bytes::from(vec![6]));
        block_builder.add_entry(key::new("Kia", 1), Bytes::from(vec![7]));
        let block = block_builder.build();

        let encoded = block.encode(&Arc::new(LsmOptions::default()));

        let decoded_block_to_test = Block::decode(&encoded, &Arc::new(LsmOptions::default()))
            .unwrap();

        assert_eq!(decoded_block_to_test.get_key_by_index(0).to_string(), String::from("Jaime"));
        assert_eq!(decoded_block_to_test.get_value_by_index(0), vec![1]);
        assert_eq!(decoded_block_to_test.get_key_by_index(1).to_string(), String::from("Javier"));
        assert_eq!(decoded_block_to_test.get_value_by_index(1), vec![2]);
        assert_eq!(decoded_block_to_test.get_key_by_index(2).to_string(), String::from("Jose"));
        assert_eq!(decoded_block_to_test.get_value_by_index(2), vec![3]);
        assert_eq!(decoded_block_to_test.get_key_by_index(3).to_string(), String::from("Juan"));
        assert_eq!(decoded_block_to_test.get_value_by_index(3), vec![4]);
        assert_eq!(decoded_block_to_test.get_key_by_index(4).to_string(), String::from("Justo"));
        assert_eq!(decoded_block_to_test.get_value_by_index(4), vec![5]);
        assert_eq!(decoded_block_to_test.get_key_by_index(5).to_string(), String::from("Justoo"));
        assert_eq!(decoded_block_to_test.get_value_by_index(5), vec![6]);
        assert_eq!(decoded_block_to_test.get_key_by_index(6).to_string(), String::from("Kia"));
        assert_eq!(decoded_block_to_test.get_value_by_index(6), vec![7]);
    }
}