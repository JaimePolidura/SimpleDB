use std::sync::Arc;
use bytes::Bytes;
use crate::block::block_decoder::decode_block;
use crate::block::block_encoder::encode_block;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::utils::utils;

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

    pub fn decode(encoded: &Vec<u8>, options: &Arc<LsmOptions>) -> Result<Block, ()> {
        decode_block(encoded, options)
    }

    pub fn get_value(&self, key_lookup: &Key) -> Option<bytes::Bytes> {
        let mut left = 0;
        let mut right = self.offsets.len() / 2;

        loop {
            let current_index = (left + right) / 2;
            let current_key = self.get_key_by_index(current_index);

            if left == right {
                return None;
            }
            if current_key.eq(key_lookup) {
                return Some(self.get_value_by_index(current_index));
            }
            if current_key.gt(key_lookup) {
                right = current_index;
            }
            if current_key.lt(key_lookup) {
                left = current_index;
            }
        }
    }

    //Expect n_entry_index to be an index to block::offsets aray
    pub fn get_key_by_index(&self, n_entry_index: usize) -> Key {
        let entry_index: usize = self.offsets[n_entry_index] as usize;
        let key_length: usize = utils::u8_vec_to_u16_le(&self.entries, entry_index) as usize;
        let key_slice: &[u8] = &self.entries[entry_index + 2..(key_length + entry_index + 2)];
        let key = String::from_utf8(key_slice.to_vec())
            .expect("Error while parsing with UTF-8");

        Key::new(key.as_str())
    }

    //Expect n_entry_index to be an index to block::offsets aray
    pub fn get_value_by_index(&self, n_entry_index: usize) -> Bytes {
        let entry_index = self.offsets[n_entry_index];
        let key_length = utils::u8_vec_to_u16_le(&self.entries, entry_index as usize);
        let value_index = (entry_index as usize) + 2 + key_length as usize;
        let value_length = utils::u8_vec_to_u16_le(&self.entries, value_index) as usize;

        Bytes::copy_from_slice(&self.entries[(value_index + 2)..((value_index + 2) + value_length)])
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use bytes::Bytes;
    use crate::block::block::Block;
    use crate::block::block_builder::BlockBuilder;
    use crate::key::Key;
    use crate::lsm_options::LsmOptions;

    #[test]
    fn encode_and_decode() {
        let mut block_builder = BlockBuilder::new(Arc::new(LsmOptions::default()));
        block_builder.add_entry(Key::new("Jaime"), Bytes::from(vec![1]));
        block_builder.add_entry(Key::new("Javier"), Bytes::from(vec![2]));
        block_builder.add_entry(Key::new("Jose"), Bytes::from(vec![3]));
        block_builder.add_entry(Key::new("Juan"), Bytes::from(vec![4]));
        block_builder.add_entry(Key::new("Justo"), Bytes::from(vec![5]));
        block_builder.add_entry(Key::new("Justoo"), Bytes::from(vec![6]));
        block_builder.add_entry(Key::new("Kia"), Bytes::from(vec![7]));
        let block = block_builder.build();

        let encoded = block.encode(&Arc::new(LsmOptions::default()));

        let decoded_block_to_test = Block::decode(&encoded, &Arc::new(LsmOptions::default()))
            .unwrap();

        println!("Hola");

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