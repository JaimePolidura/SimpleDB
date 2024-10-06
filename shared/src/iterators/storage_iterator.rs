use bytes::Bytes;
use crate::key::Key;

//This is the iterator interface for all iterators in simpleDb, specially in the storage engine layer
//The iterated collection is expected:
// - To be ordered in ascending order
// - Items to be unique
pub trait StorageIterator {
    //Returns true if it has advanced
    fn next(&mut self) -> bool;

    fn has_next(&self) -> bool;

    //Returns current key. Expect call after next();
    fn key(&self) -> &Key;

    //Returns current value. Expect call after next();
    fn value(&self) -> &[u8];

    //Expect call after creation of the iterator
    //Expect call to next() after calling seek(), to get the seeked value with call key() & value()
    //[1, 3, 5] Seek = 1, inclusive = true. The iterator will point to 1
    //[1, 3, 5] Seek = 3, inclusive = false. The iterator will point to 1
    //[1, 3, 5] Seek = 6, inclusive = true or false. The iterator will be emtpy
    //[1, 3, 5] Seek = 5, inclusive = false. The iterator will be emtpy
    //[1, 3, 5] Seek = 0, inclusive = true or false. The iterator will point to 1
    //[1, 3, 5] Seek = 2, inclusive = true or false. The iterator will point to 3
    fn seek(&mut self, key: &Bytes, inclusive: bool);
}