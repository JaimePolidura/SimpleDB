use crate::key::Key;

pub trait StorageIterator {
    fn next(&mut self) -> bool;

    fn has_next(&self) -> bool;

    fn key(&self) -> &Key;

    fn value(&self) -> &[u8];
}