use crate::key;
use crate::key::Key;
use crate::memtables::memtable::MemTable;
use crate::transactions::transaction::Transaction;
use crate::utils::storage_iterator::StorageIterator;
use bytes::Bytes;
use std::collections::Bound::Excluded;
use std::ops::Bound::Included;
use std::sync::Arc;

//This iterators fulfills:
// - The returned keys are readble/visible by the current transaction.
// - The returned key's bytes might be returned multiple times.
//
//   For example (byess, txn_id): (A, 1), (A, 2), (A, 3) with iterator txn_id = 2,
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

    //Expect next() call after seek_key(), in order to get the seeked valuae

    pub fn seek_key(&mut self, key: &Bytes) {
        let key = key::create(key.clone(), 0);
        if let Some(prev_entry_to_key) = self.memtable.data.upper_bound(Excluded(&key)) {
            self.current_value = Some(prev_entry_to_key.value().clone());
            self.current_key = Some(prev_entry_to_key.key().clone());
        } else {
            //Key higher than max key of the map, the iterator should return false in has next
            if self.is_higher(&key) && !self.memtable.data.is_empty() {
                let max_entry = self.memtable.data.back().unwrap().clone();
                self.current_value = Some(max_entry.value().clone());
                self.current_key = Some(max_entry.key().clone());
            }
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

impl<'a> StorageIterator for MemtableIterator {
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
}

#[cfg(test)]
mod test {
    use crate::key;
    use crate::memtables::memtable::MemTable;
    use crate::memtables::memtable_iterator::MemtableIterator;
    use crate::transactions::transaction::Transaction;
    use crate::transactions::transaction_manager::IsolationLevel;
    use crate::utils::storage_iterator::StorageIterator;
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    fn iterators_seekkey() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0).unwrap());
        memtable.set_active();
        let value: Vec<u8> = vec![10, 12];
        memtable.set(&transaction(1), Bytes::from("B"), &value);
        memtable.set(&transaction(2), Bytes::from("B"), &value);
        memtable.set(&transaction(3), Bytes::from("B"), &value);
        memtable.set(&transaction(4), Bytes::from("D"), &value);
        memtable.set(&transaction(5), Bytes::from("D"), &value);
        memtable.set(&transaction(6), Bytes::from("F"), &value);
        memtable.set(&transaction(7), Bytes::from("F"), &value);

        // Start from the beggining [B, D, F] Seek: A
        let mut iterator_ = MemtableIterator::create(&memtable, &Transaction::none());
        iterator_.seek_key(&Bytes::from("A"));
        assert!(iterator_.has_next());
        iterator_.next();
        assert!(iterator_.key().eq(&key::create_from_str("B", 1)));

        // Start from D [B, D, F] Seek: D
        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());
        iterator.seek_key(&Bytes::from("D"));
        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("D", 4)));

        // Start from D [B, D, F] Seek: C
        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());
        iterator.seek_key(&Bytes::from("D"));
        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("D", 4)));

        // Out of bounds [B, D, F] Seek: G
        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());
        iterator.seek_key(&Bytes::from("G"));
        assert!(!iterator.has_next());
    }

    #[test]
    fn iterators_readuncommited() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0).unwrap());
        memtable.set_active();
        let value: Vec<u8> = vec![10, 12];
        memtable.set(&transaction(1), Bytes::from("alberto"), &value);
        memtable.set(&transaction(2), Bytes::from("alberto"), &value);
        memtable.set(&transaction(3), Bytes::from("alberto"), &value);
        memtable.set(&transaction(1), Bytes::from("jaime"), &value);
        memtable.set(&transaction(5), Bytes::from("jaime"), &value);
        memtable.set(&transaction(1), Bytes::from("gonchi"), &value);
        memtable.set(&transaction(0), Bytes::from("wili"), &value);

        let mut iterator = MemtableIterator::create(&memtable, &Transaction::none());

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("alberto", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("alberto", 2)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("alberto", 3)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("gonchi", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("jaime", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("jaime", 5)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("wili", 0)));

        assert!(!iterator.has_next());
    }

    #[test]
    fn iterators_snapshotisolation() {
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0).unwrap());
        memtable.set_active();
        let value: Vec<u8> = vec![10, 12];
        memtable.set(&transaction(10), Bytes::from("aa"), &value); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("alberto"), &value);
        memtable.set(&transaction(2), Bytes::from("alberto"), &value);
        memtable.set(&transaction(4), Bytes::from("alberto"), &value); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("gonchi"), &value);
        memtable.set(&transaction(5), Bytes::from("javier"), &value); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(1), Bytes::from("jaime"), &value);
        memtable.set(&transaction(5), Bytes::from("jaime"), &value); //Cannot be read by the transaction, should be ignored
        memtable.set(&transaction(0), Bytes::from("wili"), &value);

        let mut iterator = MemtableIterator::create(&memtable, &transaction_with_iso(3, IsolationLevel::SnapshotIsolation));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("alberto", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("alberto", 2)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("gonchi", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("jaime", 1)));

        assert!(iterator.has_next());
        iterator.next();
        assert!(iterator.key().eq(&key::create_from_str("wili", 0)));
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