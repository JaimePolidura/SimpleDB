use std::sync::Arc;
use bytes::{BufMut, Bytes};
use crate::sst::block::block::{Block, BLOCK_FOOTER_LENGTH};
use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::utils::utils;

pub struct BlockBuilder {
    entries: Vec<Entry>,
    current_size: usize,
    options: Arc<LsmOptions>,
}

struct Entry {
    key: Key,
    value: Bytes,
}

impl BlockBuilder {
    pub fn new(options: Arc<LsmOptions>) -> BlockBuilder {
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
            let offset = entries.len();

            //Key
            entries.put_u16_le(entry.key.len() as u16);
            entries.put_u64_le(entry.key.txn_id() as u64);
            entries.extend(entry.key.as_bytes());
            //Value
            entries.put_u16_le(entry.value.len() as u16);
            entries.extend(entry.value.as_ref());

            offsets.push(offset as u16);
        }

        Block { entries, offsets }
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

    fn calculate_entry_size(&self, key: &Key, value: &Bytes) -> usize {
        std::mem::size_of::<i16>() + //Key length size
            std::mem::size_of::<u64>() + //Key txn id
            key.len() + //Key bytes
            std::mem::size_of::<i16>() + //Value length
            value.len() + //Value bytes
            std::mem::size_of::<i16>()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use crate::sst::block::block_builder::BlockBuilder;
    use crate::lsm_options::LsmOptions;
    use bytes::Bytes;
    use crate::key;

    #[test]
    fn build() {
        let mut block_builder = BlockBuilder::new(Arc::new(LsmOptions::default()));
        block_builder.add_entry(key::new("Jaime", 1), Bytes::from(vec![1, 2, 3]));
        block_builder.add_entry(key::new("Pedro", 1), Bytes::from(vec![4, 5, 6]));
        let block = block_builder.build();

        assert_eq!(block.get_value_by_index(0), vec![1, 2, 3]);
        assert_eq!(block.get_key_by_index(0).to_string(), String::from("Jaime"));

        assert_eq!(block.get_value_by_index(1), vec![4, 5, 6]);
        assert_eq!(block.get_key_by_index(1).to_string(), String::from("Pedro"));
    }
}