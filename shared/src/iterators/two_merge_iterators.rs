use crate::iterators::storage_iterator::StorageIterator;
use bytes::Bytes;
use crate::key::Key;

pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,

    choose_a: bool,

    first_iteration: bool,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(mut a: A, mut b: B) -> TwoMergeIterator<A, B> {
        TwoMergeIterator { a, b, choose_a: false, first_iteration: true }
    }

    fn choose_a(&self) -> bool {
        if !self.a.has_next() && !self.b.has_next() {
            //Return previous choice
            return self.choose_a;
        }
        if !self.a.has_next() && self.b.has_next() {
            return false;
        }
        if !self.b.has_next() && self.a.has_next() {
            return true;
        }

        self.a.key() < self.b.key()
    }

    fn skip_b_duplicates(&mut self) {
        while self.a.has_next() && self.b.has_next() && self.a.key() == self.b.key() {
            self.b.next();
        }
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn next(&mut self) -> bool {
        if self.first_iteration {
            self.first_iteration = false;
            self.a.next();
            self.b.next();
            self.skip_b_duplicates();
            self.choose_a = self.choose_a();
            return true;
        }

        let mut advanced: bool = false;

        if self.choose_a {
            advanced = self.a.next();
        } else { //Choose b
            advanced = self.b.next();
        }

        self.skip_b_duplicates();
        self.choose_a = self.choose_a();

        advanced
    }

    fn has_next(&self) -> bool {
        self.a.has_next() || self.b.has_next()
    }

    fn key(&self) -> &Key {
        if self.choose_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.choose_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    //Expect call after creation
    fn seek(&mut self, key: &Bytes, inclusive: bool) {
        self.a.seek(key, inclusive);
        self.b.seek(key, inclusive);
        self.first_iteration = true;
    }
}

#[cfg(test)]
mod test {
    use crate::iterators::mock_iterator::MockIterator;
    use crate::iterators::two_merge_iterators::TwoMergeIterator;
    use bytes::Bytes;
    use crate::assertions;
    use crate::iterators::storage_iterator::StorageIterator;

    // A -> B -> D
    // A -> C -> D -> F
    #[test]
    fn seek_exclusive_contained_2() {
        let mut iterator = create_iterator();
        iterator.seek(&Bytes::from("d"), false);

        assertions::assert_iterator_str_seq(
            iterator,
            vec![
                "f"
            ]
        );
    }

    // A -> B -> D
    // A -> C -> D -> F
    #[test]
    fn seek_exclusive_contained() {
        let mut iterator = create_iterator();
        iterator.seek(&Bytes::from("c"), false);

        assertions::assert_iterator_str_seq(
            iterator,
            vec![
                "d",
                "f"
            ]
        );
    }

    // A -> B -> D
    // A -> C -> D -> F
    #[test]
    fn seek_inclusive_contained() {
        let mut iterator = create_iterator();
        iterator.seek(&Bytes::from("b"), true);

        assertions::assert_iterator_str_seq(
            iterator,
            vec![
                "b",
                "c",
                "d",
                "f"
            ]
        );
    }

    // A -> B -> D
    // A -> C -> D -> F
    #[test]
    fn iterator() {
        assertions::assert_iterator_str_seq(
            create_iterator(),
            vec![
                "a",
                "b",
                "c",
                "d",
                "f"
            ]
        );
    }

    fn create_iterator() -> TwoMergeIterator<MockIterator, MockIterator> {
        let mut iterator1 = MockIterator::create();
        iterator1.add_entry("a", 0, Bytes::from(vec![1]));
        iterator1.add_entry("b", 0, Bytes::from(vec![1]));
        iterator1.add_entry("d", 0, Bytes::from(vec![1]));

        let mut iterator2 = MockIterator::create();
        iterator2.add_entry("a", 0, Bytes::from(vec![1]));
        iterator2.add_entry("c", 0, Bytes::from(vec![3]));
        iterator2.add_entry("d", 0, Bytes::from(vec![4]));
        iterator2.add_entry("f", 0, Bytes::from(vec![5]));

        TwoMergeIterator::create(iterator1, iterator2)
    }
}