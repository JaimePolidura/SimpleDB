use crate::key::Key;
use crate::utils::storage_iterator::StorageIterator;

pub struct MergeIterator<I: StorageIterator> {
    iterators: Vec<Box<I>>,

    current_iterator: Option<Box<I>>,
    current_iterator_index: usize,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn new(iterators: Vec<Box<I>>) -> MergeIterator<I> {
        MergeIterator {
            current_iterator_index: 0,
            current_iterator: None,
            iterators,
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn next(&mut self) -> bool {
        self.iterators.insert(self.current_iterator_index, self.current_iterator.take().unwrap());

        let mut min_key_seen: Option<&Key> = None;
        let mut min_key_seen_index: usize = 0;
        let mut current_index: i32 = -1;
        for iterator in &self.iterators {
            current_index = current_index + 1;

            if iterator.has_next() {
                let current_key = iterator.key();

                match min_key_seen {
                    Some(_) => if current_key.le(min_key_seen.unwrap()) {
                        min_key_seen_index = current_index as usize;
                        min_key_seen = Some(current_key);
                    },
                    None => min_key_seen = Some(current_key),
                }
            }
        }

        let some_key_found = min_key_seen.is_some();

        if some_key_found {
            self.current_iterator = Some(self.iterators.remove(min_key_seen_index));
        }

        some_key_found
    }

    fn has_next(&self) -> bool {
        let some_iterator_has_next = self.iterators.iter().filter(|i| i.has_next()).count() > 0;
        let current_iterator_has_next = self.current_iterator.is_some() && self.current_iterator
            .as_ref()
            .unwrap()
            .has_next();

        some_iterator_has_next || current_iterator_has_next
    }

    fn key(&self) -> &Key {
        self.current_iterator
            .as_ref()
            .expect("Illegal merge iterator state")
            .key()
    }

    fn value(&self) -> &[u8] {
        self.current_iterator
            .as_ref()
            .expect("Illegal merge iterator state")
            .value()
    }
}