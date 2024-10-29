use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
use crate::sst::block::block::{Block, BLOCK_FOOTER_LENGTH, LAST_OVERFLOW_BLOCK, NORMAL_BLOCK, OVERFLOW_BLOCK};
use bytes::{BufMut, Bytes};
use shared::key::Key;
use std::sync::Arc;

pub struct BlockBuilder {
    entries: Vec<Entry>,
    current_size_bytes: usize,
    options: Arc<shared::SimpleDbOptions>,
    keyspace_desc: KeyspaceDescriptor
}

struct Entry {
    key: Key,
    value: Bytes,
}

impl BlockBuilder {
    pub fn create(options: Arc<shared::SimpleDbOptions>, keyspace_desc: KeyspaceDescriptor) -> BlockBuilder {
        BlockBuilder {
            current_size_bytes: BLOCK_FOOTER_LENGTH,
            entries: Vec::new(),
            keyspace_desc,
            options,
        }
    }

    //Returns err if the block cannot contain more values. This occurs when the max block size have been exceeded
    pub fn add_entry(&mut self, key: &Key, value: &Bytes) -> Result<(), ()> {
        let new_size = self.current_size_bytes + self.calculate_entry_size(&key, &value);

        //The block is full, there is no room for a new key
        if self.current_size_bytes + self.calculate_key_size(&key) >= self.options.block_size_bytes {
            return Err(());
        }
        //This entry overflows a block size
        if self.does_entry_overflows_block(&key, &value) {
            self.entries.push(Entry { key: key.clone(), value: value.clone() });
            return Err(());
        }
        //The entry doesn't overflow the block, but its size + current size of the block exceeds the max block size
        //the new entry should be added in the next block
        if new_size > self.options.block_size_bytes {
            return Err(());
        }

        self.entries.push(Entry { key: key.clone(), value: value.clone() });
        self.current_size_bytes = new_size;

        Ok(())
    }

    //This returns a vec of blocks, just in case one value of an entry overflows the max block size
    pub fn build(&self) -> Vec<Block> {
        let mut current_offsets: Vec<u16> = Vec::new();
        let mut current_entries: Vec<u8> = Vec::new();
        let mut current_size_block = BLOCK_FOOTER_LENGTH;

        for entry in &self.entries {
            //The overflow entry is the last entry added to the block_builder
            if self.does_entry_overflows_block(&entry.key, &entry.value) {
                return self.build_overflow_blocks(current_size_block, current_offsets, current_entries, entry);
            } else {
                let offset = current_entries.len();
                current_size_block += self.calculate_entry_size(&entry.key, &entry.value);

                //Key
                current_entries.extend(entry.key.serialize());
                //Value
                current_entries.put_u16_le(entry.value.len() as u16);
                current_entries.extend(entry.value.as_ref());
                current_offsets.push(offset as u16);
            }
        }

        vec![Block {
            keyspace_desc: self.keyspace_desc,
            entries: current_entries,
            offsets: current_offsets,
            flag: NORMAL_BLOCK,
        }]
    }

    fn build_overflow_blocks(
        &self,
        mut current_block_size: usize,
        mut current_block_offsets: Vec<u16>,
        mut current_block_entries: Vec<u8>,
        overflow_entry: &Entry
    ) -> Vec<Block> {
        let mut blocks_built = Vec::new();

        let current_overflow_value_bytes_to_write = overflow_entry.value.len();
        let mut current_overflow_value_bytes_written = 0;

        while current_overflow_value_bytes_written < current_overflow_value_bytes_to_write {
            //Can add part of the overflow bytes in the current block?
            if current_block_size + self.calculate_key_size(&overflow_entry.key) < self.options.block_size_bytes {
                let value_size_bytes_available_to_write = self.options.block_size_bytes - current_block_size - self.calculate_key_size(&overflow_entry.key);
                current_overflow_value_bytes_written += value_size_bytes_available_to_write;
                let bytes_to_write = overflow_entry.value.slice(
                    current_overflow_value_bytes_written..
                     (current_overflow_value_bytes_written + value_size_bytes_available_to_write)
                );

                //Write to block
                let offset = current_block_entries.len();
                current_block_entries.extend(overflow_entry.key.serialize());
                current_block_entries.put_u16_le(bytes_to_write.len() as u16);
                current_block_entries.extend(bytes_to_write);
                current_block_offsets.push(offset as u16);

                let is_last_block = current_overflow_value_bytes_written == current_overflow_value_bytes_to_write;
                blocks_built.push(Block {
                    keyspace_desc: self.keyspace_desc,
                    entries: current_block_entries.clone(),
                    offsets: current_block_offsets.clone(),
                    flag: if is_last_block { OVERFLOW_BLOCK } else { LAST_OVERFLOW_BLOCK } ,
                });

                current_block_size = BLOCK_FOOTER_LENGTH;
                current_block_entries = Vec::new();
                current_block_offsets = Vec::new();
            } else {
                //Build key & value to the first block
                let offset = current_block_entries.len();
                current_block_size += self.calculate_key_size(&overflow_entry.key);
                current_block_entries.extend(overflow_entry.key.serialize());
                current_block_offsets.push(offset as u16);

                blocks_built.push(Block {
                    keyspace_desc: self.keyspace_desc,
                    entries: current_block_entries.clone(),
                    offsets: current_block_offsets.clone(),
                    flag: OVERFLOW_BLOCK,
                });

                current_block_size = BLOCK_FOOTER_LENGTH;
                current_block_entries = Vec::new();
                current_block_offsets = Vec::new();
            }
        }

        blocks_built
    }

    fn does_entry_overflows_block(&self, key: &Key, value: &Bytes) -> bool {
        let entry_size = self.calculate_entry_size(&key, &value);
        entry_size + BLOCK_FOOTER_LENGTH > self.options.block_size_bytes
    }

    //Calculates only the size of the key in the block
    fn calculate_key_size(&self, key: &Key) -> usize {
        key.serialized_size() + //Key bytes
            std::mem::size_of::<i16>() //Offset entry
    }

    //Calculates the full entry size: Offset size + value size
    fn calculate_entry_size(&self, key: &Key, value: &Bytes) -> usize {
        key.serialized_size() + //Key size
            std::mem::size_of::<i16>() + //Value length
            value.len() + //Value bytes
            std::mem::size_of::<i16>() //Offsets entry
    }
}

#[cfg(test)]
mod test {
    use crate::keyspace::keyspace_descriptor::KeyspaceDescriptor;
    use crate::sst::block::block_builder::BlockBuilder;
    use bytes::Bytes;
    use shared::key::Key;
    use shared::Type;
    use std::sync::Arc;

    #[test]
    fn build() {
        let mut block_builder = BlockBuilder::create(Arc::new(shared::SimpleDbOptions::default()), KeyspaceDescriptor::create_mock(Type::String));
        block_builder.add_entry(&Key::create_from_str("Jaime", 1), &Bytes::from(vec![1, 2, 3]));
        block_builder.add_entry(&Key::create_from_str("Pedro", 1), &Bytes::from(vec![4, 5, 6]));
        let mut block = block_builder.build();
        let block = block[0].clone(); //Get the first block, ex

        assert_eq!(block.get_value_by_index(0).0, vec![1, 2, 3]);
        assert_eq!(block.get_key_by_index(0).to_string(), String::from("Jaime"));

        assert_eq!(block.get_value_by_index(1).0, vec![4, 5, 6]);
        assert_eq!(block.get_key_by_index(1).to_string(), String::from("Pedro"));
    }
}