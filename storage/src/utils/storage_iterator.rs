use crate::key::Key;

pub trait StorageIterator {
    //Returns true if it has advanced
    fn next(&mut self) -> bool;

    fn has_next(&self) -> bool;

    //Returns current key. Expect call after next();
    fn key(&self) -> &Key;

    //Returns current value. Expect call after next();
    fn value(&self) -> &[u8];
}