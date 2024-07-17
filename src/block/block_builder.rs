use bytes::{BufMut, Bytes};
use crate::block::block::{Block, BLOCK_FOOTER_LENGTH};
use crate::key::Key;
use crate::lsm_options::LsmOptions;

pub struct BlockBuilder {
    entries: Vec<Entry>,
    current_size: usize,
    options: LsmOptions,
}

struct Entry {
    key: Key,
    value: Bytes,
}

impl BlockBuilder {
    pub fn new(options: LsmOptions) -> BlockBuilder {
        BlockBuilder {
            entries: Vec::new(),
            current_size: BLOCK_FOOTER_LENGTH,
            options,
        }
    }

    pub fn build(&self) -> Block {
        let mut offsets: Vec<u16> = Vec::new();
        let mut entries: Vec<u8> = Vec::new();

        for entry in &self.entries {
            let offset: u16 = entries.len() as u16;

            self.add_u16_length_to_bytes(&mut entries, entry.key.len() as u16);
            entries.put_slice(entry.key.as_bytes());
            self.add_u16_length_to_bytes(&mut entries, entry.value.len() as u16);
            entries.put_slice(entry.value.as_ref());

            offsets.push(offset);
        }

        Block{ entries, offsets }
    }

    //TODO Handle block overflow
    pub fn add_entry(&mut self, key: Key, value: Bytes) -> Result<(), ()> {
        let entry_size = self.calculate_entry_size(&key, &value);
        let new_size = self.current_size + entry_size;

        if new_size > self.options.block_size_bytes {
            return Err(());
        }

        self.entries.push(Entry { key, value });
        self.current_size = new_size;

        Ok(())
    }

    fn add_u16_length_to_bytes(&self, bytes: &mut Vec<u8>, length: u16) {
        bytes.push(length.to_le_bytes()[0]);
    }

    fn calculate_entry_size(&self, key: &Key, value: &Bytes) -> usize {
        return std::mem::size_of::<i16>() + //Key length size
            key.len() + //Key bytes
            std::mem::size_of::<i16>() + //Value length
            value.len() + //Value bytes
            std::mem::size_of::<i16>() //Entry Offset
        ;
    }
}