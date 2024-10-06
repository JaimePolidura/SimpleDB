use crate::iterators::seek_iterator::SeekIterator;
use crate::iterators::storage_iterator::StorageIterator;
use bytes::Bytes;
use crate::key::Key;

pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    choose_a: bool,
    current_value_a: bool,
    first_iteration: bool,

}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(mut a: A, mut b: B) -> TwoMergeIterator<A, B> {
        TwoMergeIterator { a, b, choose_a: false, current_value_a: false, first_iteration: true }
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
            self.a.next();
            self.b.next();
            let choose_a = Self::choose_a(&self.a, &self.b);
            self.current_value_a = choose_a;
            self.choose_a = choose_a;

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

impl<A: StorageIterator + SeekIterator, B: StorageIterator + SeekIterator> SeekIterator for TwoMergeIterator<A, B> {
    //Expect call after creation
    fn seek(&mut self, key: &Bytes, inclusive: bool) {
        self.a.seek(key, inclusive);
        self.b.seek(key, inclusive);

        let choose_a = Self::choose_a(&self.a, &self.b);
        self.current_value_a = choose_a;
        self.choose_a = choose_a;
    }
}

#[cfg(test)]
mod test {
    use crate::iterators::mock_iterator::MockIterator;
    use crate::iterators::two_merge_iterators::TwoMergeIterator;
    use bytes::Bytes;
    use crate::assertions;

    #[test]
    fn two_merge_iterator() {
        let mut iterator1 = MockIterator::create();
        iterator1.add_entry("a", 0, Bytes::from(vec![1]));
        iterator1.add_entry("b", 0, Bytes::from(vec![1]));
        iterator1.add_entry("d", 0, Bytes::from(vec![1]));

        let mut iterator2 = MockIterator::create();
        iterator2.add_entry("a", 0, Bytes::from(vec![1]));
        iterator2.add_entry("c", 0, Bytes::from(vec![3]));
        iterator2.add_entry("d", 0, Bytes::from(vec![4]));
        iterator2.add_entry("f", 0, Bytes::from(vec![5]));

        assertions::assert_iterator_key_value_seq(
            TwoMergeIterator::create(iterator1, iterator2),
            vec![
                ("a", vec![1]),
                ("b", vec![2]),
                ("c", vec![3]),
                ("d", vec![4]),
            ]
        );
    }
}