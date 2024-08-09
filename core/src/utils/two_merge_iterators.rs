use crate::key::Key;
use crate::utils::storage_iterator::StorageIterator;

pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    choose_a: bool,
    current_value_a: bool,
    first_iteration: bool,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn new(mut a: A, mut b: B) -> TwoMergeIterator<A, B> {
        a.next();
        b.next();
        let choose_a = Self::choose_a(&a, &b);
        let current_value_a = choose_a;

        TwoMergeIterator { a, b, choose_a, current_value_a, first_iteration: true }
    }

    fn choose_a(a: &A, b: &B) -> bool {
        if !a.has_next() {
            return false;
        }
        if !b.has_next() {
            return true;
        }

        a.key() > b.key()
    }

    fn skip_b_duplicates(&mut self) {
        while self.a.has_next() && self.b.has_next() && self.a.key() == self.b.key() {
            self.b.next();
        }
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn next(&mut self) -> bool {
        //As StorageIterator::new calls next(), we dont want to call it twice from the users code
        if self.first_iteration {
            self.first_iteration = false;
            return self.has_next();
        }

        let mut advanced: bool = false;

        if self.choose_a {
            advanced = self.a.next();
            self.current_value_a = true;
        } else { //Choose b
            advanced = self.b.next();
            self.current_value_a = false;
        }

        self.skip_b_duplicates();
        self.choose_a = Self::choose_a(&self.a, &self.b);

        advanced
    }

    fn has_next(&self) -> bool {
        self.a.has_next() || self.b.has_next()
    }

    fn key(&self) -> &Key {
        if self.current_value_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.current_value_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use crate::key::Key;
    use crate::lsm_options::LsmOptions;
    use crate::memtables::memtable::{MemTable, MemtableIterator};
    use crate::utils::storage_iterator::StorageIterator;
    use crate::utils::two_merge_iterators::TwoMergeIterator;

    #[test]
    fn two_merge_iterator() {
        let memtable1 = Arc::new(MemTable::new(&LsmOptions::default(), 0));
        memtable1.set(&Key::new("a"), &vec![1]);
        memtable1.set(&Key::new("b"), &vec![2]);
        memtable1.set(&Key::new("d"), &vec![4]);

        let memtable2 = Arc::new(MemTable::new(&LsmOptions::default(), 0));
        memtable1.set(&Key::new("a"), &vec![1]);
        memtable1.set(&Key::new("c"), &vec![3]);
        memtable1.set(&Key::new("d"), &vec![4]);
        memtable1.set(&Key::new("f"), &vec![5]);

        let mut two_merge_iterators = TwoMergeIterator::new(
            MemtableIterator::new(&memtable1),
            MemtableIterator::new(&memtable2),
        );

        assert!(two_merge_iterators.has_next());
        two_merge_iterators.next();
        assert!(two_merge_iterators.key().eq(&Key::new("a")));
        assert!(two_merge_iterators.value().eq(&vec![1]));

        assert!(two_merge_iterators.has_next());
        two_merge_iterators.next();
        assert!(two_merge_iterators.key().eq(&Key::new("b")));
        assert!(two_merge_iterators.value().eq(&vec![2]));

        assert!(two_merge_iterators.has_next());
        two_merge_iterators.next();
        assert!(two_merge_iterators.key().eq(&Key::new("c")));
        assert!(two_merge_iterators.value().eq(&vec![3]));

        assert!(two_merge_iterators.has_next());
        two_merge_iterators.next();
        assert!(two_merge_iterators.key().eq(&Key::new("d")));
        assert!(two_merge_iterators.value().eq(&vec![4]));

        assert!(two_merge_iterators.has_next());
        two_merge_iterators.next();
        assert!(two_merge_iterators.key().eq(&Key::new("f")));
        assert!(two_merge_iterators.value().eq(&vec![5]));

        assert!(!two_merge_iterators.has_next());
    }
}