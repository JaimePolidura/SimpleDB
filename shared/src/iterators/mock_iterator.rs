use crate::iterators::storage_iterator::StorageIterator;
use crate::key::Key;
use crate::TxnId;
use bytes::Bytes;

pub struct MockIterator {
    entries: Vec<(Key, Bytes)>,

    next_index: usize,
}

impl MockIterator {
    pub fn create_from(
        entries: Vec<&str>
    ) -> MockIterator {
        let mut iterator = Self::create();
        for entry in entries {
            iterator.add_entry(entry, 0, Bytes::from(vec![0]));
        }
        iterator
    }

    pub fn create() -> MockIterator {
        MockIterator {
            entries: Vec::new(),
            next_index: 0
        }
    }

    pub fn add_entry(&mut self, key: &str, txn_id: TxnId, value: Bytes) {
        self.entries.push((Key::create_from_str(key, txn_id), value));
    }
}

impl StorageIterator for MockIterator {
    fn next(&mut self) -> bool {
        if self.next_index < self.entries.len() {
            self.next_index += 1;
            true
        } else {
            false
        }
    }

    fn has_next(&self) -> bool {
        self.next_index < self.entries.len()
    }

    fn key(&self) -> &Key {
        let (k, _) = self.entries.get(self.next_index - 1).unwrap();
        k
    }

    fn value(&self) -> &[u8] {
        let (_, v) = self.entries.get(self.next_index - 1).unwrap();
        v
    }

    fn seek(&mut self, to_seek: &Bytes, inclusive: bool) {
        let mut last_key: Option<Key> = None;

        if self.entries.is_empty() {
            return;
        }
        if self.entries[self.entries.len() - 1].0.bytes_lt_bytes(to_seek) ||
            (self.entries[self.entries.len() - 1].0.bytes_le_bytes(to_seek) && !inclusive)
        {
            self.next_index = self.entries.len() + 1;
            return;
        }

        for current_index in 0..self.entries.len() {
            let (current_key, _) = self.entries.get(current_index)
                .unwrap();
            if inclusive && current_key.bytes_eq_bytes(to_seek) {
                self.next_index = current_index;
                return;
            } else if !inclusive && current_key.bytes_eq_bytes(to_seek) {
                self.next_index = current_index + 1;
                return;
            }

            match last_key.take() {
                Some(last_key_from_optional) => {
                    if last_key_from_optional.bytes_lt_bytes(to_seek) && current_key.bytes_gt_bytes(to_seek) {
                        self.next_index = current_index; //[1, 3, 4] Seek = 2, iterator will point to 3
                        return;
                    } else {
                        last_key = Some(current_key.clone());
                    }
                }
                None => {
                    if current_key.bytes_gt_bytes(to_seek) {
                        self.next_index = 0; //[1, 2, 3] Seek = 0, iterator will point to 1
                        return;
                    } else {
                        last_key = Some(current_key.clone());
                    }
                },
            };
        }
    }
}

#[cfg(test)]
mod test {
    use crate::assertions;
    use crate::iterators::mock_iterator::MockIterator;
    use bytes::Bytes;
    use crate::iterators::storage_iterator::StorageIterator;

    #[test]
    fn seek_not_contained() {
        let mut mock_iterator = MockIterator::create_from(vec!["a", "c", "d", "f"]);
        mock_iterator.seek(&Bytes::from("b"), true);
        assertions::assert_iterator_str_seq(
            mock_iterator,
            vec!["c", "d", "f"]
        );
    }

    #[test]
    fn seek_higherbound_inclusive() {
        let mut mock_iterator = MockIterator::create_from(vec!["b", "c"]);
        mock_iterator.seek(&Bytes::from("c"), true);
        assertions::assert_iterator_str_seq(
            mock_iterator,
            vec!["c"]
        );
    }

    #[test]
    fn seek_higherbound_exclusive() {
        let mut mock_iterator = MockIterator::create_from(vec!["b", "c"]);
        mock_iterator.seek(&Bytes::from("d"), true);
        assertions::assert_empty_iterator(mock_iterator);
    }

    #[test]
    fn seek_lowerbound() {
        let mut mock_iterator = MockIterator::create_from(vec!["b", "c"]);
        mock_iterator.seek(&Bytes::from("a"), true);
        assertions::assert_iterator_str_seq(
            mock_iterator,
            vec!["b", "c"]
        );
    }

    #[test]
    fn seek_exclusive() {
        let mut mock_iterator = MockIterator::create_from(vec!["a", "c", "d", "f"]);
        mock_iterator.seek(&Bytes::from("c"), false);
        assertions::assert_iterator_str_seq(
            mock_iterator,
            vec!["d", "f"]
        );
    }

    #[test]
    fn seek_inclusive() {
        let mut mock_iterator = MockIterator::create_from(vec!["a", "c", "d", "f"]);
        mock_iterator.seek(&Bytes::from("c"), true);
        assertions::assert_iterator_str_seq(
            mock_iterator,
            vec!["c", "d", "f"]
        );
    }

    #[test]
    fn iterator() {
        assertions::assert_iterator_str_seq(
            MockIterator::create_from(vec!["a", "c", "d"]),
            vec!["a", "c", "d"]
        );
    }
}