use bytes::BufMut;

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
        let mut bitmap: Vec<u8> = vec![0, n_vec_slots as u8];

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