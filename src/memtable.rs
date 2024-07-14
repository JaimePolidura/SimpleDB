use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use crossbeam_skiplist::SkipMap;
use crate::lsm_options::LsmOptions;

pub struct MemTable {
    data: SkipMap<bytes::Bytes, bytes::Bytes>,
    current_size_bytes: AtomicUsize,
    max_size_bytes: usize,
}

impl MemTable {
    pub fn new(options: &LsmOptions) -> MemTable {
        MemTable {
            max_size_bytes: options.memtable_max_size_bytes,
            current_size_bytes: AtomicUsize::new(0),
            data: SkipMap::new(),
        }
    }
    
    pub fn get(&self, key: &[u8]) -> Option<&bytes::Bytes> {
        // TODO
        // self.data.get(key)
        //     .map(|r| { r.value()})
        None
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<(), ()> {
        self.write_into_skip_list(
            bytes::Bytes::copy_from_slice(key),
            bytes::Bytes::copy_from_slice(value)
        )
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), ()> {
        self.write_into_skip_list(
            bytes::Bytes::copy_from_slice(key),
            bytes::Bytes::new()
        )
    }

    fn write_into_skip_list(&self, key: bytes::Bytes, value: bytes::Bytes) -> Result<(), ()> {
        if self.current_size_bytes.load(Relaxed) >= self.max_size_bytes {
            return Err(());
        }

        self.current_size_bytes.fetch_add(key.len() + value.len(), Relaxed);

        self.data.insert(
            key, value
        );

        Ok(())
    }
}
