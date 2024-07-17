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

    fn encode_entries(&self, encoded: &mut Vec<u8>) {
        encoded.extend(&self.entries);
    }

    fn encode_offsets(&self, encoded: &mut Vec<u8>) -> u16 {
        let offsetts_offset_xd = encoded.len() as u16;
        encoded.extend(utils::u16_vec_to_u8_vec(&self.offsets));
        offsetts_offset_xd
    }

    fn encode_footer(
        &self,
        start_offsets_offset: u16,
        encoded: &mut Vec<u8>,
        options: LsmOptions
    ) {
        //Nº Entries
        let n_entries: u16 = self.entries.len() as u16;
        encoded[options.memtable_max_size_bytes - 4] = (n_entries & 0xff) as u8;
        encoded[options.memtable_max_size_bytes - 3] = (n_entries >> 8 & 0xff) as u8;

        //Entrie's offsets start offset XD
        encoded[options.memtable_max_size_bytes - 2] = (n_entries & 0xff) as u8;
        encoded[options.memtable_max_size_bytes - 1] = (start_offsets_offset >> 8 & 0xff) as u8;
    }

    pub fn decode(blocks: Vec<u8>) -> Result<Block, ()> {

    }
}