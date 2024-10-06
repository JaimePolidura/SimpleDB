use crate::memtables::memtable::MemTable;
use crate::transactions::transaction::Transaction;
use bytes::Bytes;
use shared::iterators::storage_iterator::StorageIterator;
use shared::key::Key;
use shared::MAX_TXN_ID;
use std::collections::Bound::Excluded;
use std::ops::Bound::Included;
use std::sync::Arc;

//This iterators fulfills:
// - The returned keys are readable/visible by the current transaction.
// - The returned key's bytes might be returned multiple times.
//
//   For example (bytes, txn_id): (A, 1), (A, 2), (A, 3) with iterator txn_id = 2,
//   the iterator will return: (A, 1) and (A, 2)
pub struct MemtableIterator {
    memtable: Arc<MemTable>,

    current_value: Option<Bytes>,
    current_key: Option<Key>,

    transaction: Transaction,
}

impl MemtableIterator {
    pub fn create(memtable: &Arc<MemTable>, transaction: &Transaction) -> MemtableIterator {
        MemtableIterator {
            transaction: transaction.clone(),
            memtable: memtable.clone(),
            current_value: None,
            current_key: None,
        }
    }

    fn get_next_readable_key(&self) -> Option<(Key, Bytes)> {
        let mut current_key = self.current_key.clone();

        loop {
            match self.get_next_key(&current_key) {
                Some((key, value)) => {
                    if self.transaction.can_read(&key) {
                        return Some((key, value));
                    } else {
                        current_key = Some(key);
                    }
                },
                None => {
                    return None
                }
            }
        }
    }

    fn get_next_key(&self, prev_key: &Option<Key>) -> Option<(Key, Bytes)> {
        if self.memtable.data.is_empty() {
            return None;
        }

        match prev_key {
            Some(current_key) => {
                if let Some(entry) = self.memtable.data.lower_bound(Excluded(&current_key)) {
                    Some((entry.key().clone(), entry.value().clone()))
                } else {
                    None
                }
            },
            None => {
                let entry = self.memtable.data.iter().next()
                    .unwrap();
                Some((entry.key().clone(), entry.value().clone()))
            },
        }
    }

    fn is_higher(&self, key: &Key) -> bool {
        if let Some(max_entry) = self.memtable.data.back() {
            return max_entry.key().lt(key)
        }
        false
    }
}

impl StorageIterator for MemtableIterator {
    fn next(&mut self) -> bool {
        match self.get_next_readable_key() {
            Some((next_key, next_value)) => {
                self.current_key = Some(next_key);
                self.current_value = Some(next_value);
                true
            },
            None => false
        }
    }

    fn has_next(&self) -> bool {
        self.get_next_readable_key().is_some()
    }

    fn key(&self) -> &Key {
        self.current_key
            .as_ref()
            .expect("Illegal iterator state")
    }

    fn value(&self) -> &[u8] {
        self.current_value
            .as_ref()
            .expect("Illegal iterator state")
    }

    fn seek(&mut self, key_bytes: &Bytes, inclusive: bool) {
        let key_txn_id = if inclusive { self.transaction.txn_id } else { MAX_TXN_ID };
        let key = Key::create(key_bytes.clone(), key_txn_id);
        let bound = if inclusive { Included(&key) } else { Excluded(&key) };

        if let Some(seeked_entry) = self.memtable.data.lower_bound(bound) {
            //Set current key to point to the previous key before entry_from_bound.key(), so that
            //we will need to call next() after seek() to get the seeked value
            if let Some(prev_entry_to_seeked) = self.memtable.data.upper_bound(Excluded(seeked_entry.key())) {
                self.current_value = Some(prev_entry_to_seeked.value().clone());
                self.current_key = Some(prev_entry_to_seeked.key().clone());
            } else {
                self.current_value = None;
                self.current_key = None;
            }
        } else {
            //Key higher than max key of the map, the iterator should return false in has next
            if self.is_higher(&key) && !self.memtable.data.is_empty() {
                let max_entry = self.memtable.data.back().unwrap().clone();
                self.current_value = Some(max_entry.value().clone());
                self.current_key = Some(max_entry.key().clone());
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::memtables::memtable::MemTable;
    use crate::memtables::memtable_iterator::MemtableIterator;
    use crate::transactions::transaction::Transaction;
    use crate::transactions::transaction_manager::IsolationLevel;
    use bytes::Bytes;
    use shared::assertions::assert_iterator_key_seq;
    use shared::iterators::storage_iterator::StorageIterator;
    use shared::key::Key;
    use std::sync::Arc;
    use shared::assertions;

    #[test]
    fn iterators_seek() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0, 0).unwrap());
        memtable.set_active();
        memtable.set(&transaction(1), Bytes::from("B"), &vec![]);
        memtable.set(&transaction(2), Bytes::from("B"), &vec![]);
        memtable.set(&transaction(3), Bytes::from("B"), &vec![]);
        memtable.set(&transaction(4), Bytes::from("D"), &vec![]);
        memtable.set(&transaction(5), Bytes::from("D"), &vec![]);
        memtable.set(&transaction(6), Bytes::from("F"), &vec![]);
        memtable.set(&transaction(7), Bytes::from("F"), &vec![]);

        //[B, D, F] Seek: A, Inclusive
        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());
        iterator.seek(&Bytes::from("A"), true);
        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&Key::create_from_str("B", 1)));

        //[B, D, F] Seek: D, Inclusive
        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());
        iterator.seek(&Bytes::from("D"), true);
        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&Key::create_from_str("D", 4)));

        //[B, D, F] Seek: D, Exclusive
        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());
        iterator.seek(&Bytes::from("D"), false);
        assert!(iterator.has_next());
        iterator.next();
        assert_eq!(iterator.key().clone(), Key::create_from_str("F", 6));

        //[B, D, F] Seek: G, Inclusive
        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());
        iterator.seek(&Bytes::from("G"), true);
        assertions::assert_empty_iterator(iterator);

        //[B, D, F] Seek: F, Exclusive
        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());
        iterator.seek(&Bytes::from("F"), false);
        assertions::assert_empty_iterator(iterator);
    }

    #[test]
    fn iterators_read_uncommited() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0, 0)
            .unwrap());
        memtable.set(&transaction(1), Bytes::from("alberto"), &vec![]);
        memtable.set(&transaction(2), Bytes::from("alberto"), &vec![]);
        memtable.set(&transaction(3), Bytes::from("alberto"), &vec![]);
        memtable.set(&transaction(1), Bytes::from("jaime"), &vec![]);
        memtable.set(&transaction(5), Bytes::from("jaime"), &vec![]);
        memtable.set(&transaction(1), Bytes::from("gonchi"), &vec![]);
        memtable.set(&transaction(0), Bytes::from("wili"), &vec![]);

        assert_iterator_key_seq(
            MemtableIterator::create(&memtable, &Transaction::none()),
            vec![
                Key::create_from_str("alberto", 1),
                Key::create_from_str("alberto", 2),
                Key::create_from_str("alberto", 3),
                Key::create_from_str("gonchi", 1),
                Key::create_from_str("jaime", 1),
                Key::create_from_str("jaime", 5),
                Key::create_from_str("wili", 0)
            ]
        );
    }

    #[test]
    fn iterators_snapshot_isolation() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0, 0)
            .unwrap());
        memtable.set_active();
        memtable.set(&transaction(10), Bytes::from("aa"), &vec![]); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("alberto"), &vec![]);
        memtable.set(&transaction(2), Bytes::from("alberto"), &vec![]);
        memtable.set(&transaction(4), Bytes::from("alberto"), &vec![]); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("gonchi"), &vec![]);
        memtable.set(&transaction(5), Bytes::from("javier"), &vec![]); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("jaime"), &vec![]);
        memtable.set(&transaction(5), Bytes::from("jaime"), &vec![]); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(0), Bytes::from("wili"), &vec![]);

        assert_iterator_key_seq(
            MemtableIterator::create(&memtable, &transaction_with_iso(3, IsolationLevel::SnapshotIsolation)),
            vec![
                Key::create_from_str("alberto", 1),
                Key::create_from_str("alberto", 2),
                Key::create_from_str("gonchi", 1),
                Key::create_from_str("jaime", 1),
                Key::create_from_str("wili", 0)
            ]
        );
    }

    fn transaction(txn_id: shared::TxnId) -> Transaction {
        let mut transaction = Transaction::none();
        transaction.txn_id = txn_id;
        transaction
    }

    fn transaction_with_iso(txn_id: shared::TxnId, isolation_level: IsolationLevel) -> Transaction {
        let mut transaction = Transaction::none();
        transaction.isolation_level = isolation_level;
        transaction.txn_id = txn_id;
        transaction
    }
}