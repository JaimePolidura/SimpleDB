use std::collections::VecDeque;
use bytes::Bytes;
use shared::seek_iterator::SeekIterator;
use shared::TxnId;
use crate::key;
use crate::key::Key;
use crate::utils::storage_engine_iterator::StorageEngineIterator;
use crate::utils::storage_iterator::StorageIterator;

pub struct MockIterator {
    entries: VecDeque<(Key, Bytes)>,

    current_key: Option<Key>,
    current_value: Option<Bytes>,
}

impl MockIterator {
    pub fn create() -> MockIterator {
        MockIterator {
            entries: VecDeque::new(),
            current_value: None,
            current_key: None
        }
    }

    pub fn add_entry(&mut self, key: &str, txn_id: TxnId, value: Bytes) {
        self.entries.push_back((key::create_from_str(key, txn_id), value));
    }
}

impl StorageIterator for MockIterator {
    fn next(&mut self) -> bool {
        match self.entries.pop_front() {
            Some(entry) => {
                self.current_value = Some(entry.1);
                self.current_key = Some(entry.0);
                true
            }
            None => false
        }
    }

    fn has_next(&self) -> bool {
        !self.entries.is_empty()
    }

    fn key(&self) -> &Key {
        self.current_key.as_ref().unwrap()
    }

    fn value(&self) -> &[u8] {
        self.current_value.as_ref().unwrap()
    }
}

impl SeekIterator for MockIterator {
    fn seek(&mut self, to_seek: &Bytes, inclusive: bool) {
        while let Some((current_key, current_value)) = self.entries.pop_front() {
            if inclusive && current_key.bytes_eq_bytes(to_seek) {
                self.current_value = Some(current_value);
                self.current_key = Some(current_key);
                return;
            } else if !inclusive && current_key.bytes_eq_bytes(to_seek) {
                if let Some((next_key, next_value)) = self.entries.pop_front() {
                    self.current_value = Some(next_value);
                    self.current_key = Some(next_key);
                }
                return;
            }
        }
    }
}