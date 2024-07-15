pub trait StorageIterator {
    fn next(&mut self) -> bool;

    fn has_next(&self) -> bool;

    fn key(&self) -> &[u8];

    fn value(&self) -> &[u8];
}