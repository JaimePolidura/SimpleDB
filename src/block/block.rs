use bytes::Bytes;
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::utils::utils;

//Nº Entries + Offset of entrie's offset in block
pub const BLOCK_FOOTER_LENGTH: usize = std::mem::size_of::<u16>() + std::mem::size_of::<u16>();

pub struct Block {
    pub(crate) entries: Vec<u8>,
    pub(crate) offsets: Vec<u16>
}

impl Block {
    pub fn encode(self, options: LsmOptions) -> Vec<u8> {
        let mut encoded: Vec<u8> = Vec::with_capacity(options.block_size_bytes);

        self.encode_entries(&mut encoded);
        let start_offsets_offset = self.encode_offsets(&mut encoded);
        self.encode_footer(start_offsets_offset, &mut encoded, options);

        encoded
    }

    pub fn get_key(&self, n_entry_index: usize) -> Key {
        let entry_index: usize = self.offsets[n_entry_index] as usize;
        let key_length: usize = utils::u8_to_u16_le(&self.entries, entry_index) as usize;
        let key_slice: &[u8] = &self.entries[entry_index + 2..(key_length + entry_index + 2)];
        let key = String::from_utf8(key_slice.to_vec())
            .expect("Error while parsing with UTF-8");

        Key::new(key.as_str())
    }

    pub fn get_value(&self, n_entry_index: usize) -> Bytes {
        let entry_index: u16 = self.offsets[n_entry_index];
        let key_length: u16 = utils::u8_to_u16_le(&self.entries, entry_index as usize);
        let value_index = (entry_index as usize) + 2 + key_length as usize;
        let value_length = utils::u8_to_u16_le(&self.entries, value_index) as usize;

        Bytes::copy_from_slice(&self.entries[(value_index + 2)..value_length])
    }

    fn encode_entries(&self, encoded: &mut Vec<u8>) {
        encoded.extend(&self.entries);
    }

    fn encode_offsets(&self, encoded: &mut Vec<u8>) -> usize {
        let offsetts_offset_xd = encoded.len();
        encoded.extend(utils::u16_vec_to_u8_vec(&self.offsets));
        offsetts_offset_xd
    }

    fn encode_footer(
        &self,
        start_offsets_offset: usize,
        encoded: &mut Vec<u8>,
        options: LsmOptions
    ) {
        let n_entries: u16 = self.entries.len() as u16;
        utils::u16_to_u8_le(n_entries, options.memtable_max_size_bytes - 4, encoded);

        utils::u16_to_u8_le(start_offsets_offset as u16, options.memtable_max_size_bytes - 2, encoded);
    }

    pub fn decode(encoded: &Vec<u8>, options: LsmOptions) -> Result<Block, ()> {
        if encoded.len() != options.block_size_bytes {
            return Err(());
        }

        let offsets_offset: u16 = utils::u8_to_u16_le(&encoded, options.memtable_max_size_bytes - 2);
        let n_entries: u16 = utils::u8_to_u16_le(&encoded, options.memtable_max_size_bytes - 4);

        let offsets = Self::decode_offsets(encoded, offsets_offset, n_entries);
        let entries = Self::decode_entries(encoded, offsets_offset);

        Ok(Block{ offsets, entries })
    }

    fn decode_offsets(
        encoded: &Vec<u8>,
        offsets_offset: u16,
        n_entries: u16,
    ) -> Vec<u16> {
        let start_index = offsets_offset as usize;
        let end_inedx = start_index + (n_entries * std::mem::size_of::<u16>() as u16) as usize;

        utils::u8_vec_to_u16_vec(&encoded[start_index..end_inedx].to_vec())
    }

    fn decode_entries(
        encoded: &Vec<u8>,
        offsets_offset: u16,
    ) -> Vec<u8> {
        let start_index = 0;
        let end_index = offsets_offset as usize;

        encoded[start_index..=end_index].to_vec()
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use crate::block::block::Block;
    use crate::block::block_builder::BlockBuilder;
    use crate::key::Key;
    use crate::lsm_options::LsmOptions;

    #[test]
    fn encode_and_decode() {
        let mut block_builder = BlockBuilder::new(LsmOptions::default());
        block_builder.add_entry(Key::new("Jaime"), Bytes::from(vec![1, 2, 3]));
        block_builder.add_entry(Key::new("Pedro"), Bytes::from(vec![4, 5, 6]));
        let block = block_builder.build();

        let encoded = block.encode(LsmOptions::default());
        let decoded_block_to_test = Block::decode(&encoded, LsmOptions::default());

        assert_eq!(decoded_block_to_test.get_value(0), vec![1, 2, 3]);
        assert_eq!(decoded_block_to_test.get_key(0).to_string(), String::from("Jaime"));

        assert_eq!(decoded_block_to_test.get_value(1), vec![4, 5, 6]);
        assert_eq!(decoded_block_to_test.get_key(1).to_string(), String::from("Pedro"));
    }
}