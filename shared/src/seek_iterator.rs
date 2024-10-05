use bytes::Bytes;

pub trait SeekIterator {
    fn seek(&mut self, key: &Bytes, inclusive: bool) -> bool;
}