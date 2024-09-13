use std::cmp::max;
use std::sync::Arc;
use bytes::Bytes;
use crate::sst::block::block_decoder::decode_block;
use crate::sst::block::block_encoder::encode_block;
use crate::key;
use crate::key::Key;
use crate::transactions::transaction::{Transaction};

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
    pub fn encode(&self, options: &Arc<shared::SimpleDbOptions>) -> Vec<u8> {
        encode_block(&self, options)
    }

    pub fn decode(encoded: &Vec<u8>, options: &Arc<shared::SimpleDbOptions>) -> Result<Block, shared::DecodeErrorType> {
        decode_block(encoded, options)
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        let max_key = self.get_key_by_index(self.offsets.len() - 1);
        let min_key = self.get_key_by_index(0);
        min_key.le(key) && max_key.ge(key)
    }

    pub fn get_value(&self, key_lookup: &Bytes, transaction: &Transaction) -> Option<Bytes> {
        let (value, _) = self.binary_search_by_key(key_lookup, transaction);
        value
    }

    //Only used by block_iterator
    // [A, B, D], key_lookup = B, returned = 0
    // [A, B, D], key_lookup = C, returned = 1
    pub(crate) fn get_key_iterator_index(&self, key_lookup: &Bytes) -> usize {
        let (value_found, index) = self.binary_search_by_key(key_lookup, &Transaction::none());
        match value_found {
            Some(_) => {
                if index == 0 {
                    index
                } else {
                    index - 1
                }
            },
            None => index,
        }
    }

    //Optinoal of value bytes, index of last search
    fn binary_search_by_key(&self, key_lookup: &Bytes, transaction: &Transaction) -> (Option<(Bytes)>, usize) {
        let mut right = self.offsets.len();
        let mut left = 0;

        while left < right {
            let current_index = (left + right) / 2;
            let mut current_key = self.get_key_by_index(current_index);

            if left == right {
                return (None, current_index);
            }
            if current_key.bytes_eq_bytes(key_lookup) {
                return self.get_value_in_multiple_key_versions(transaction, key_lookup, current_index);
            }
            if current_key.bytes_gt_bytes(key_lookup) {
                right = current_index;
            }
            if current_key.bytes_lt_bytes(key_lookup) {
                left = current_index + 1;
            }
        }

        (None, left)
    }

    //Different versions exists for the same key
    fn get_value_in_multiple_key_versions(
        &self,
        transaction: &Transaction,
        key: &Bytes,
        index: usize
    ) -> (Option<(Bytes)>, usize) { //Byte values, index of alst search
        let mut current_index = index;
        while current_index > 0 && self.get_key_by_index(current_index - 1).bytes_eq_bytes(key) {
            current_index = current_index - 1;
        }

        while current_index < self.entries.len() {
            let current_key = self.get_key_by_index(current_index);
            if current_key.bytes_eq_bytes(key) {
                return (None, index);
            }
            if transaction.can_read(&current_key) {
                return (Some(self.get_value_by_index(current_index)), current_index);
            }
        }

        (None, index)
    }

    //Expect n_entry_index to be an index to block::offsets aray
    pub fn get_key_by_index(&self, n_entry_index: usize) -> Key {
        let entry_index = self.offsets[n_entry_index] as usize;
        let key_length = shared::u8_vec_to_u16_le(&self.entries, entry_index) as usize;
        let key_txn_id = shared::u8_vec_to_u64_le(&self.entries, entry_index + 2) as shared::TxnId;
        let key_bytes = self.entries[entry_index + 10..(key_length + entry_index + 10)].to_vec();

        key::create(Bytes::from(key_bytes), key_txn_id)
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

    #[test]
    fn encode_and_decode() {
        let mut block_builder = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()));
        block_builder.add_entry(key::create_from_str("Jaime", 1), Bytes::from(vec![1]));
        block_builder.add_entry(key::create_from_str("Javier", 1), Bytes::from(vec![2]));
        block_builder.add_entry(key::create_from_str("Jose", 1), Bytes::from(vec![3]));
        block_builder.add_entry(key::create_from_str("Juan", 1), Bytes::from(vec![4]));
        block_builder.add_entry(key::create_from_str("Justo", 1), Bytes::from(vec![5]));
        block_builder.add_entry(key::create_from_str("Justoo", 1), Bytes::from(vec![6]));
        block_builder.add_entry(key::create_from_str("Kia", 1), Bytes::from(vec![7]));
        let block = block_builder.build();

        let encoded = block.encode(&Arc::new(shared::SimpleDbOptions::default()));

        let decoded_block_to_test = Block::decode(&encoded, &Arc::new(shared::SimpleDbOptions::default()))
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