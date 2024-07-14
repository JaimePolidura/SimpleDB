use crate::lsm_options::LsmOptions;
use crate::memtables::Memtables;

pub struct Lsm {
    options: LsmOptions,
    memtables: Memtables
}

impl Lsm {
    pub fn new(lsm_options: LsmOptions) -> Lsm {
        Lsm {
            options: lsm_options,
            memtables: Memtables::new(lsm_options)
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&bytes::Bytes> {
        self.memtables.get(key)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        self.memtables.set(key, value);
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.memtables.delete(key);
    }
}