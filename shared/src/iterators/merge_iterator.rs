use crate::iterators::storage_iterator::StorageIterator;
use crate::key::Key;
use bytes::Bytes;
use std::collections::HashSet;

#[derive(Clone)]
pub struct MergeIterator<I: StorageIterator> {
    //We use Option so that when an iterator has not next, we can remove it by placing None
    iterators: Vec<Option<Box<I>>>,

    last_value_iterated: Option<Bytes>,
    last_key_iterated: Option<Key>,
    finished_iterators_indexes: HashSet<usize>,

    first_iteration: bool,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(mut iterators: Vec<Box<I>>) -> MergeIterator<I> {
        let mut iterators_options = Vec::new();
        while !iterators.is_empty() {
            let iterator = iterators.remove(0);
            if iterator.has_next() {
                iterators_options.push(Some(iterator));
            }
        }

        MergeIterator {
            finished_iterators_indexes: HashSet::new(),
            iterators: iterators_options,
            last_value_iterated: None,
            last_key_iterated: None,
            first_iteration: true,
        }
    }

    fn advance_iterators(
        &mut self,
        min_key_seen: &Key
    ) {
        let mut finished_iterators = Vec::new();

        for current_index in 0..self.iterators.len() {
            if self.finished_iterators_indexes.contains(&current_index) {
                continue;
            }

            let iterator = &mut self.iterators[current_index];

            if iterator.is_some() {
                let iterator = iterator.as_mut().unwrap();
                while !is_iterator_up_to_date(&iterator, &min_key_seen) {
                    if !iterator.next() { //Has not advanced
                        finished_iterators.push(current_index);
                        break;
                    }
                }
            }
        }

        self.remove_finished_iterators(finished_iterators);
    }

    fn remove_finished_iterators(&mut self, finished_iterators_index: Vec<usize>) {
        for finished_iterator_index in finished_iterators_index {
            self.finished_iterators_indexes.insert(finished_iterator_index);
        }
    }

    fn call_next_on_every_iterator(&mut self) {
        let mut current_index = 0;

        for iterator in &mut self.iterators {
            let iterator = iterator.as_mut().unwrap();
            //Has not advanced
            if !iterator.next() {
                self.finished_iterators_indexes.insert(current_index);
            }

            current_index += 1;
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn next(&mut self) -> bool {
        if self.first_iteration {
            self.call_next_on_every_iterator();
            self.first_iteration = false;
        }

        let mut min_key_seen: Option<Key> = None;
        let mut min_iterator_index = 0;

        for (current_index, current_iterator) in &mut self.iterators.iter().enumerate() {
            if self.finished_iterators_indexes.contains(&current_index) {
                continue;
            }

            if let Some(current_iterator) = current_iterator {
                let current_key = current_iterator.key();

                let key_smaller_than_prev_iteration: bool = self.last_key_iterated.is_some() &&
                    current_key <= &self.last_key_iterated.as_mut().unwrap();
                let key_larger_than_min: bool = min_key_seen.is_some() &&
                    current_key >= min_key_seen.as_ref().unwrap();

                if !key_smaller_than_prev_iteration && !key_larger_than_min {
                    match min_key_seen {
                        Some(_) => if current_key.le(min_key_seen.as_ref().unwrap()) {
                            min_key_seen = Some(current_key.clone());
                            min_iterator_index = current_index;
                        },
                        None => {
                            min_key_seen = Some(current_key.clone());
                            min_iterator_index = current_index;
                        },
                    }
                }
            }
        }

        let some_key_found = min_key_seen.is_some();
        if some_key_found {
            self.last_key_iterated = min_key_seen.clone();
            self.last_value_iterated = Some(Bytes::copy_from_slice(&self.iterators[min_iterator_index]
                .as_ref()
                .unwrap()
                .value()));

            self.advance_iterators(min_key_seen.as_ref().unwrap());
        }

        some_key_found
    }

    fn has_next(&self) -> bool {
        self.finished_iterators_indexes.len() < self.iterators.len()
    }

    fn key(&self) -> &Key {
        self.last_key_iterated
            .as_ref()
            .expect("Illegal merge iterator state")
    }

    fn value(&self) -> &[u8] {
        self.last_value_iterated
            .as_ref()
            .expect("Illegal merge iterator state")
    }

    fn seek(&mut self, key: &Bytes, inclusive: bool) {
        for iterator in &mut self.iterators {
            if iterator.is_some() {
                let iterator = iterator.as_mut().unwrap();
                iterator.seek(key, inclusive);
            }
        }

        self.first_iteration = true;
    }
}

fn is_iterator_up_to_date<I: StorageIterator>(it: &Box<I>, last_key: &Key) -> bool {
    it.key() > last_key
}

#[cfg(test)]
mod test {
    use crate::assertions;
    use crate::assertions::assert_iterator_str_seq;
    use crate::iterators::merge_iterator::MergeIterator;
    use crate::iterators::mock_iterator::MockIterator;
    use crate::iterators::storage_iterator::StorageIterator;
    use bytes::Bytes;

    #[test]
    fn one_entry() {
        assert_iterator_str_seq(
            MergeIterator::create(vec![Box::new(
                MockIterator::create_from_strs_values(vec!["a"])
            )]),
            vec!["a"]);
    }

    #[test]
    fn emtpy() {
        let mut iterator = MergeIterator::create(vec![Box::new(MockIterator::create())]);
        //No effect
        iterator.seek(&Bytes::from("e"), false);
        assertions::assert_empty_iterator(iterator);
    }

    /**
    A -> B -> D
    B -> E
    C -> D -> E
    */
    #[test]
    fn seek4() {
        let mut iterator = create_merge_iterator();
        iterator.seek(&Bytes::from("e"), false);
        iterator.next();
        assertions::assert_empty_iterator(iterator);
    }

    /**
    A -> B -> D
    B -> E
    C -> D -> E
    */
    #[test]
    fn seek3() {
        let mut iterator = create_merge_iterator();
        iterator.seek(&Bytes::from("e"), true);

        assert_iterator_str_seq(
            iterator,
            vec!["e"]
        );
    }

    /**
    A -> B -> D
    B -> E
    C -> D -> E
    */
    #[test]
    fn seek2() {
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
    fn seek1() {
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
        iterator1.add_entry("a", 0, Bytes::from("a"));
        iterator1.add_entry("b", 0, Bytes::from("b"));
        iterator1.add_entry("d", 0, Bytes::from("d"));

        let mut iterator2 = MockIterator::create();
        iterator2.add_entry("b", 0, Bytes::from("b"));
        iterator2.add_entry("e", 0, Bytes::from("e"));

        let mut iterator3 = MockIterator::create();
        iterator3.add_entry("c", 0, Bytes::from("c"));
        iterator3.add_entry("d", 0, Bytes::from("d"));
        iterator3.add_entry("e", 0, Bytes::from("e"));

        MergeIterator::create(vec![
            Box::new(iterator1),
            Box::new(iterator2),
            Box::new(iterator3)
        ])
    }
}