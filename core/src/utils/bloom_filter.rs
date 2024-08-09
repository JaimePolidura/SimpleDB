use bytes::BufMut;
use crate::utils::utils;

pub struct BloomFilter {
    bitmap: Vec<u8>,
}

impl BloomFilter {
    pub fn may_contain(&self, hash: u32) -> bool {
        let slot_index = hash & (self.bitmap.len() - 1) as u32;
        let bit_index_in_slot = hash & (std::mem::size_of::<u8>() - 1) as u32;
        let slot: u8 = self.bitmap[slot_index as usize];

        slot >> bit_index_in_slot & 0x01 == 0x01
    }

    pub fn encode(&self) -> Vec<u8> {
        let crc = crc32fast::hash(&self.bitmap);
        let mut encoded: Vec<u8> = Vec::with_capacity(self.bitmap.len() + 8);
        encoded.put_u32_le(crc);
        encoded.put_u32_le(self.bitmap.len() as u32);
        encoded.extend(&self.bitmap);
        encoded
    }

    pub fn decode(bytes: &Vec<u8>, start_offset: usize) -> Result<BloomFilter, ()> {
        let expected_crc = utils::u8_vec_to_u32_le(bytes, start_offset);
        let n_bytes = utils::u8_vec_to_u32_le(bytes, start_offset + 4);

        let bitmap_start_index = start_offset + 8;
        let bitmap_end_index = start_offset + 8 + n_bytes as usize;
        let bloom_bitmap = bytes[bitmap_start_index..bitmap_end_index].to_vec();
        let actual_crc = crc32fast::hash(&bloom_bitmap);

        if actual_crc != expected_crc {
            return Err(());
        }

        Ok(Self::from_botmap(bloom_bitmap))
    }

    pub fn from_botmap(bitmap: Vec<u8>) -> BloomFilter {
        BloomFilter{ bitmap }
    }

    pub fn new(
        hashes: &Vec<u32>,
        n_entries: usize //Expect power of 2
    ) -> BloomFilter {
        let n_vec_slots = (n_entries / 8) as u32;
        let mut bitmap: Vec<u8> = Vec::with_capacity(n_vec_slots as usize);
        utils::fill_vec(&mut bitmap, n_vec_slots as usize, 0);

        for hash in hashes {
            let slot_index = hash & (n_vec_slots - 1);
            let bit_index_in_slot = hash & (std::mem::size_of::<u8>() - 1) as u32;
            let slot: u8 = bitmap[slot_index as usize];
            let updated_slot: u8 = slot | (0x01 << bit_index_in_slot);
            bitmap[slot_index as usize] = updated_slot;
        }

        BloomFilter { bitmap }
    }
}

#[cfg(test)]
mod test {
    use crate::utils::bloom_filter::BloomFilter;

    #[test]
    fn may_contain() {
        let hashes = vec![101212, 1389172819, 182971, 12, 1729187291];
        let bloom = BloomFilter::new(&hashes, 64);

        assert!(bloom.may_contain(101212));
        assert!(bloom.may_contain(1389172819));
        assert!(bloom.may_contain(182971));
        assert!(bloom.may_contain(12));
        assert!(bloom.may_contain(1729187291));

        assert!(!bloom.may_contain(1729187290));
    }

    #[test]
    fn decode_encode() {
        let encoded = BloomFilter::new(&vec![101212, 1389172819, 182971, 12, 1729187291], 64)
            .encode();
        let decoded_result = BloomFilter::decode(&encoded, 0);

        assert!(decoded_result.is_ok());
        let decoded_result = decoded_result.unwrap();

        assert!(decoded_result.may_contain(101212));
        assert!(decoded_result.may_contain(1389172819));
        assert!(decoded_result.may_contain(182971));
        assert!(decoded_result.may_contain(12));
        assert!(decoded_result.may_contain(1729187291));
    }
}
