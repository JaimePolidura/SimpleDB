use crate::iterators::storage_iterator::StorageIterator;
use crate::key::Key;
use bytes::Bytes;

pub struct MergeIterator<I: StorageIterator> {
    iterators: Vec<Option<Box<I>>>,

    current_iterator: Option<Box<I>>,
    current_iterator_index: usize,
    last_key_iterated: Option<Key>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(mut iterators: Vec<Box<I>>) -> MergeIterator<I> {
        call_next_on_every_iterator(&mut iterators);

        let mut iterators_options: Vec<Option<Box<I>>> = Vec::new();
        for iterator in iterators {
            iterators_options.push(Some(iterator));
        }

        MergeIterator {
            current_iterator_index: 0,
            last_key_iterated: None,
            current_iterator: None,
            iterators: iterators_options,
        }
    }

    fn advance_iterators(
        &mut self,
        min_key_seen: &Option<Key>
    ) {
        let mut current_index: i32 = -1;
        let mut iterator_indexes_to_clear: Vec<usize> = Vec::new();

        for iterator in &mut self.iterators {
            current_index = current_index + 1;
            if iterator.is_some() && !is_iterator_up_to_date(iterator.as_ref().unwrap(), &min_key_seen) {
                let iterator = iterator.as_mut().unwrap();

                while !is_iterator_up_to_date(&iterator, &min_key_seen) {
                    let has_advanced: bool = iterator.next();
                    let not_up_to_date_after_next = !is_iterator_up_to_date(&iterator, &min_key_seen);

                    if !has_advanced || not_up_to_date_after_next {
                        iterator_indexes_to_clear.push(current_index as usize);
                        break
                    }
                }
            }
        }

        //Place None
        for iterator_index_to_clear in iterator_indexes_to_clear {
            self.iterators[iterator_index_to_clear].take();
        }
    }
}

fn call_next_on_every_iterator<I: StorageIterator>(iterators: &mut Vec<Box<I>>) {
    for iterator in iterators {
        iterator.next();
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn next(&mut self) -> bool {
        if self.current_iterator.is_some() {
            //has_advanced
            if self.current_iterator.as_mut().unwrap().next() {
                self.iterators[self.current_iterator_index] = Some(self.current_iterator.take().unwrap());
            }
        }

        let mut min_key_seen: Option<Key> = None;
        let mut min_key_seen_index: usize = 0;
        let mut current_index: i32 = -1;
        for current_iterator in &self.iterators {
            current_index = current_index + 1;

            if current_iterator.is_some() {
                let current_iterator = current_iterator.as_ref().unwrap();

                let current_key = current_iterator.key();

                let key_smaller_than_prev_iteration: bool = self.last_key_iterated.is_some() &&
                    current_key <= &self.last_key_iterated.as_mut().unwrap();
                let key_larger_than_min: bool = min_key_seen.is_some() &&
                    current_key >= min_key_seen.as_ref().unwrap();

                if !key_smaller_than_prev_iteration && !key_larger_than_min {
                    min_key_seen_index = current_index as usize;

                    match min_key_seen {
                        Some(_) => if current_key.le(min_key_seen.as_ref().unwrap()) {
                            min_key_seen = Some(current_key.clone());
                        },
                        None => min_key_seen = Some(current_key.clone()),
                    }
                }
            }
        }

        let some_key_found = min_key_seen.is_some();
        if some_key_found {
            self.current_iterator = std::mem::replace(&mut self.iterators[min_key_seen_index], None);
            self.current_iterator_index = min_key_seen_index;
            self.advance_iterators(&min_key_seen);
            self.last_key_iterated = min_key_seen;
        }

        some_key_found
    }

    fn has_next(&self) -> bool {
        let some_remaining_iterator = self.iterators.iter()
            .filter(|it| it.is_some())
            .count() > 0;
        let current_iterator_has_next = self.current_iterator.is_some() && self.current_iterator
            .as_ref()
            .unwrap()
            .has_next();

        current_iterator_has_next || some_remaining_iterator
    }

    fn key(&self) -> &Key {
        self.last_key_iterated
            .as_ref()
            .expect("Illegal merge iterator state")
    }

    fn value(&self) -> &[u8] {
        self.current_iterator
            .as_ref()
            .expect("Illegal merge iterator state")
            .value()
    }

    fn seek(&mut self, key: &Bytes, inclusive: bool) {
        if let Some(current_iterator) = self.current_iterator.take() {
            self.iterators[self.current_iterator_index] = Some(current_iterator);
        }

        for iterator in &mut self.iterators {
            if iterator.is_some() {
                let iterator = iterator.as_mut().unwrap();
                iterator.seek(key, inclusive);
            }
        }
    }
}

fn is_iterator_up_to_date<I: StorageIterator>(it: &Box<I>, last_key: &Option<Key>) -> bool {
    last_key.is_some() && it.key() > last_key.as_ref().unwrap()
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use crate::assertions::assert_iterator_str_seq;
    use crate::iterators::merge_iterator::MergeIterator;
    use crate::iterators::mock_iterator::MockIterator;
    use crate::iterators::storage_iterator::StorageIterator;

    /**
    A -> B -> D
    B -> E
    C -> D -> E
    */
    #[test]
    fn seek_exclusive_contained() {
        let mut iterator = create_merge_iterator();
        iterator.seek(&Bytes::from("b"), false);

        assert_iterator_str_seq(
            iterator,
            vec!["c", "d", "e"]
        );
    }

    /**
    A -> B -> D
    B -> E
    C -> D -> E
    */
    #[test]
    fn seek_inclusive_contained() {
        let mut iterator = create_merge_iterator();
        iterator.seek(&Bytes::from("b"), true);

        assert_iterator_str_seq(
            iterator,
            vec!["b", "c", "d", "e"]
        );
    }

    /**
    A -> B -> D
    B -> E
    C -> D -> E
    */
    #[test]
    fn iterator() {
        assert_iterator_str_seq(
            create_merge_iterator(),
            vec!["a", "b", "c", "d", "e"]
        );
    }

    fn create_merge_iterator() -> MergeIterator<MockIterator> {
        let mut iterator1 = MockIterator::create();
        iterator1.add_entry("a", 0, Bytes::from(vec![1]));
        iterator1.add_entry("b", 0, Bytes::from(vec![1]));
        iterator1.add_entry("d", 0, Bytes::from(vec![1]));

        let mut iterator2 = MockIterator::create();
        iterator2.add_entry("b", 0, Bytes::from(vec![2]));
        iterator2.add_entry("e", 0, Bytes::from(vec![2]));

        let mut iterator3 = MockIterator::create();
        iterator3.add_entry("c", 0, Bytes::from(vec![3]));
        iterator3.add_entry("d", 0, Bytes::from(vec![3]));
        iterator3.add_entry("e", 0, Bytes::from(vec![3]));

        MergeIterator::create(vec![
            Box::new(iterator1),
            Box::new(iterator2),
            Box::new(iterator3)
        ])
    }
}