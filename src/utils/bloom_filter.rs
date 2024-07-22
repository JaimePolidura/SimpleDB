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
        let mut encoded = self.bitmap.clone();
        let crc = crc32fast::hash(&encoded);
        encoded.put_u32_le(crc);
        encoded
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
}
