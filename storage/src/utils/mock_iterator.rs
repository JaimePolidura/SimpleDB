use std::collections::VecDeque;
use bytes::Bytes;
use shared::TxnId;
use crate::key;
use crate::key::Key;
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