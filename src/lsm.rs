use crate::key::Key;
use crate::lsm_options::LsmOptions;
use crate::memtables::memtables::Memtables;

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

    pub fn get(&self, key: &Key) -> Option<bytes::Bytes> {
        self.memtables.get(key)
    }

    pub fn set(&mut self, key: &Key, value: &[u8]) {
        self.memtables.set(key, value);
    }

    pub fn delete(&mut self, key: &Key) {
        self.memtables.delete(key);
    }
}