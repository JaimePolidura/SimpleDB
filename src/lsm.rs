use crate::lsm_options::LsmOptions;
use crate::memtable::MemTable;
use crate::utils::atomic_shared_ref::AtomicSharedRef;

pub struct Lsm {
    memtable: AtomicSharedRef<MemTable>,
    options: LsmOptions
}

impl Lsm {
    pub fn new(options: LsmOptions) -> Lsm {
        Lsm {
            memtable: AtomicSharedRef::new(MemTable::new(&options)),
            options
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&bytes::Bytes> {
        let memtable_ref = self.memtable.load_ref();
        let value = memtable_ref.shared_ref.get(key);
        self.memtable.unload_ref(memtable_ref);

        value
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        let memtable_ptr = self.memtable.load_ref();
        match memtable_ptr.shared_ref.set(key, value) {
            Err(_) => {
                self.memtable.unload_ref(memtable_ptr);
                self.try_flush_memtable();
            },
            _ => {
                self.memtable.unload_ref(memtable_ptr);
            }
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        let memtable_ptr = self.memtable.load_ref();
        match memtable_ptr.shared_ref.delete(key) {
            Err(_) => {
                self.memtable.unload_ref(memtable_ptr);
                self.try_flush_memtable();
            },
            _ => {
                self.memtable.unload_ref(memtable_ptr);
            }
        }
    }

    fn try_flush_memtable(&mut self) {
        let new_memtable = MemTable::new(&self.options);

        match self.memtable.try_cas(new_memtable) {
            Ok(memtable_to_be_flushed) => self.flush_memtable(memtable_to_be_flushed),
            Err(_) => {}
        }
    }

    fn flush_memtable(&mut self, memtable: MemTable) {

    }
}