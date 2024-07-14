use crate::lsm_options::LsmOptions;
use crate::memtable::MemTable;

pub struct Lsm {
    memtable: MemTable
}

impl Lsm {
    pub fn new(options: LsmOptions) -> Lsm {
        Lsm {memtable: MemTable::new(&options)}
    }

    pub fn get(&self, key: &[u8]) -> Option<&bytes::Bytes> {
        self.memtable.get(key);
        None
    }

    pub fn set(&self, key: &[u8], value: &[u8]) {
        match self.memtable.set(key, value) {
            Err(_) => self.flush_memtable(),
            _ => {}
        };
    }

    pub fn delete(&self, key: &[u8]) {
        match self.memtable.delete(key) {
            Err(_) => self.flush_memtable(),
            _ => {}
        }
    }
    
    fn flush_memtable(&self) {
    }
}