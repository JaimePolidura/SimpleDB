use crate::sst::block::block_decoder::decode_block;
use crate::sst::block::block_encoder::encode_block;
use crate::transactions::transaction::Transaction;
use bytes::{Bytes};
use shared::key::Key;
use std::sync::Arc;

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
    pub fn serialize(&self, options: &Arc<shared::SimpleDbOptions>) -> Vec<u8> {
        encode_block(&self, options)
    }

    pub fn deserialize(
        encoded: &Vec<u8>,
        options: &Arc<shared::SimpleDbOptions>
    ) -> Result<Block, shared::DecodeErrorType> {
        decode_block(encoded, options)
    }

    pub fn is_key_bytes_higher(&self, key: &Key, inclusive: bool) -> bool {
        let max_key = self.get_key_by_index(self.offsets.len() - 1);
        (inclusive && key.bytes_gt_bytes(max_key.as_bytes())) || (!inclusive && key.bytes_ge_bytes(max_key.as_bytes()))
    }

    pub fn is_key_bytes_lower(&self, key: &Key, inclusive: bool) -> bool {
        let min_key = self.get_key_by_index(0);
        (inclusive && key.lt(&min_key)) || (inclusive && key.le(&min_key))
    }

    pub fn get_value(&self, key_lookup: &Bytes, transaction: &Transaction) -> Option<Bytes> {
        let (value, _) = self.binary_search_by_key_bytes(key_lookup, transaction);
        value
    }

    pub(crate) fn get_index(
        &self,
        bytes_lookup: &Bytes,
        inclusive: bool,
    ) -> usize {
        let (found, index) = self.binary_search_by_key_bytes(bytes_lookup, &Transaction::none());
        match found {
            Some(_) => {
                let mut current_index = index;

                if inclusive {
                    return current_index;
                }

                while current_index < self.offsets.len() {
                    let current_key = self.get_key_by_index(current_index);
                    if !current_key.bytes_eq_bytes(bytes_lookup) {
                        return current_index;
                    }

                    current_index += 1;
                }

                current_index
            }
            None => index
        }
    }

    //Does a binary search in the block to find an entry that has the same key and is readable by the transaction.
    //Returns the value of the entry and the index in the block.
    //The returned value is guaranteed to be readable by the transaction.
    pub(crate) fn binary_search_by_key_bytes(
        &self,
        key_lookup: &Bytes,
        transaction: &Transaction
    ) -> (Option<Bytes>, usize) {
        let mut right = self.offsets.len();
        let mut left = 0;

        while left < right {
            let current_index = (left + right) / 2;
            let current_key = self.get_key_by_index(current_index);

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

    //When doing a binary search we might find multiple versions exists for the same key,
    //so we need to return the first one that is readable by the transaction.
    //Returns the value found as an Option, and the index in the block
    fn get_value_in_multiple_key_versions(
        &self,
        transaction: &Transaction,
        key: &Bytes,
        index: usize
    ) -> (Option<Bytes>, usize) {
        //We make current_index to point to the first version of a given key bytes. Example:
        //[(A, 1), (B, 1), (B, 2), (B, 3)], given key = B, index: 3, this would make current_index to
        //have value 1 (first entry of B)
        let mut current_index = index;
        while current_index > 0 && self.get_key_by_index(current_index - 1).bytes_eq_bytes(key) {
            current_index = current_index - 1;
        }

        //Now we search the first readable value by the transaction
        while current_index < self.entries.len() {
            let current_key = self.get_key_by_index(current_index);
            if !current_key.bytes_eq_bytes(key) {
                return (None, index);
            }
            if transaction.can_read(&current_key) {
                return (Some(self.get_value_by_index(current_index)), current_index);
            }

            current_index += 1;
        }

        (None, index)
    }

    //Expect n_entry_index to be an index to block::offsets array
    pub fn get_key_by_index(&self, n_entry_index: usize) -> Key {
        let entry_index = self.offsets[n_entry_index] as usize;
        let key_ptr = &mut &self.entries[entry_index..];
        Key::deserialize(key_ptr)
    }

    //Expect n_entry_index to be an index to block::offsets array
    pub fn get_value_by_index(&self, n_entry_index: usize) -> Bytes {
        let entry_index = self.offsets[n_entry_index];
        let key_ptr = &mut &self.entries[entry_index as usize..];
        let key_serialized_size = Key::serialized_key_size(key_ptr);
        let value_index = (entry_index as usize) + key_serialized_size;
        let value_length = shared::u8_vec_to_u16_le(&self.entries, value_index) as usize;

        Bytes::copy_from_slice(&self.entries[(value_index + 2)..((value_index + 2) + value_length)])
    }
}

#[cfg(test)]
mod test {
    use crate::sst::block::block::Block;
    use crate::sst::block::block_builder::BlockBuilder;
    use bytes::Bytes;
    use shared::key::Key;
    use std::sync::Arc;

    #[test]
    fn serialize_deserialize() {
        let mut block_builder = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()));
        block_builder.add_entry(Key::create_from_str("Jaime", 1), Bytes::from(vec![1]));
        block_builder.add_entry(Key::create_from_str("Javier", 1), Bytes::from(vec![2]));
        block_builder.add_entry(Key::create_from_str("Jose", 1), Bytes::from(vec![3]));
        block_builder.add_entry(Key::create_from_str("Juan", 1), Bytes::from(vec![4]));
        block_builder.add_entry(Key::create_from_str("Justo", 1), Bytes::from(vec![5]));
        block_builder.add_entry(Key::create_from_str("Justoo", 1), Bytes::from(vec![6]));
        block_builder.add_entry(Key::create_from_str("Kia", 1), Bytes::from(vec![7]));
        let block = block_builder.build();

        let encoded = block.serialize(&Arc::new(shared::SimpleDbOptions::default()));

        let decoded_block_to_test = Block::deserialize(&encoded, &Arc::new(shared::SimpleDbOptions::default()))
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