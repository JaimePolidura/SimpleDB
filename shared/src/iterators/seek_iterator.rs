use bytes::Bytes;

pub trait SeekIterator {
    //Expect call after creation of the iterator
    //And next after seek call to get the seeked value
    // let iterator = Iterator::create();
    // iterator.seek(1, true);
    // iterator.next();
    fn seek(&mut self, key: &Bytes, inclusive: bool);
}