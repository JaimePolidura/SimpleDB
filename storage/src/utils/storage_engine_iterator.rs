use crate::transactions::transaction::Transaction;
use crate::transactions::transaction_manager::TransactionManager;
use crate::utils::tombstone::TOMBSTONE;
use bytes::Bytes;
use shared::iterators::storage_iterator::StorageIterator;
use shared::key::Key;
use shared::{Flag, StorageValueMergeResult};
use std::collections::VecDeque;
use std::sync::Arc;

//TODO Refactor this code

//This is the iterator that will be exposed to users of the storage engine:
//This iterator merges the values by the merger function defined in SimpleDbOptions
//And commits the transaction when the iterator is dropped if the iterator was created in "standalone" mode
// which means when the transaction was created only for the iterator, (for example: call to Storage::scan_from or Storage::scan_all)
pub struct StorageEngineIterator<I: StorageIterator> {
    options: Arc<shared::SimpleDbOptions>,
    inner_iterator: I,

    entries_to_return: VecDeque<(Key, Bytes)>, //We use VecDequeue so that we can pop from index 0

    current_value: Option<Bytes>,
    current_key: Option<Key>,

    transaction_manager: Option<Arc<TransactionManager>>,
    transaction: Option<Transaction>,

    is_finished: bool,

    keyspace_flags: Flag,

    first_iteration: bool,
}

impl<I: StorageIterator> StorageEngineIterator<I> {
    pub fn create(
        keyspace_flags: Flag,
        options: &Arc<shared::SimpleDbOptions>,
        iterator: I,
    ) -> StorageEngineIterator<I> {
        let mut is_finished = false;

        if !iterator.has_next() {
            is_finished = true
        }

        StorageEngineIterator {
            entries_to_return: VecDeque::new(),
            transaction_manager: None,
            inner_iterator: iterator,
            options: options.clone(),
            first_iteration: true,
            current_value: None,
            current_key: None,
            transaction: None,
            keyspace_flags,
            is_finished,
        }
    }

    pub fn set_transaction_standalone(
        &mut self,
        transaction_manager: &Arc<TransactionManager>,
        transaction: Transaction
    ) {
        self.transaction_manager = Some(transaction_manager.clone());
        self.transaction = Some(transaction);
    }

    fn find_entries(&mut self) -> bool {
        loop {
            if self.is_finished {
                return false;
            }

            self.entries_to_return.push_back((
                self.inner_iterator.key().clone(),
                Bytes::copy_from_slice(self.inner_iterator.value()))
            );

            let current_key_bytes = Bytes::copy_from_slice(self.inner_iterator.key().as_bytes());

            let mut has_next = self.inner_iterator.has_next();

            while has_next {
                self.inner_iterator.next();
                let next_key = self.inner_iterator.key();

                if next_key.bytes_eq_bytes(&current_key_bytes) {
                    self.entries_to_return.push_back((
                        self.inner_iterator.key().clone(),
                        Bytes::copy_from_slice(self.inner_iterator.value()))
                    );
                    has_next = self.inner_iterator.has_next();
                } else {
                    break
                }
            }

            if !has_next {
                self.is_finished = true;
            }

            if self.merge_entry_values() {
                return true;
            }
        }
    }

    //Returns true if it merged a value that can be returned to the user of the iterator
    fn merge_entry_values(&mut self) -> bool {
        if self.options.storage_value_merger.is_none() || self.entries_to_return.len() <= 1 {
            return self.check_some_keys_in_entries_to_return_readable();
        }

        let mut prev_merged_value: Option<(Key, Bytes)> = None;
        let merge_fn = self.options.storage_value_merger.unwrap();

        while let Some((next_key, next_value)) = self.entries_to_return.pop_front() {
            match prev_merged_value.take() {
                Some((_, previous_merged_value)) => {
                    match merge_fn(&previous_merged_value, &next_value, self.keyspace_flags) {
                        StorageValueMergeResult::Ok(merged_value) => prev_merged_value = Some((next_key, merged_value)),
                        StorageValueMergeResult::DiscardPreviousKeepNew => prev_merged_value = Some((next_key, next_value)),
                        StorageValueMergeResult::DiscardPreviousAndNew => {}
                    }
                },
                None => {
                    prev_merged_value = Some((next_key, next_value))
                }
            }
        }

        let (final_key, final_value) = prev_merged_value.take().unwrap();
        self.entries_to_return.push_front((final_key, final_value));
        self.check_some_keys_in_entries_to_return_readable()
    }

    fn check_some_keys_in_entries_to_return_readable(&self) -> bool {
        for (_, value) in &self.entries_to_return {
            if !value.eq(&TOMBSTONE) {
                return true;
            }
        }

        false
    }

    fn do_do_next(&mut self) -> bool {
        if self.is_finished {
            return false;
        }
        if self.entries_to_return.is_empty() && !self.find_entries() {
            return false;
        }

        let (next_key, next_value) = self.entries_to_return.pop_front().unwrap();
        self.current_value = Some(next_value);
        self.current_key = Some(next_key);

        true
    }
}

impl<I: StorageIterator> StorageIterator for StorageEngineIterator<I> {
    fn next(&mut self) -> bool {
        if self.first_iteration {
            self.inner_iterator.next();
            self.first_iteration = false;
        }

        self.do_do_next()
    }

    fn has_next(&self) -> bool {
        todo!()
    }

    fn key(&self) -> &Key {
        self.current_key.as_ref().unwrap()
    }

    fn value(&self) -> &[u8] {
        self.current_value.as_ref().unwrap()
    }

    fn seek(&mut self, key: &Bytes, inclusive: bool) {
        self.inner_iterator.seek(key, inclusive);
    }
}

impl<I: StorageIterator> Drop for StorageEngineIterator<I> {
    fn drop(&mut self) {
        if let Some(transaction_manager) = self.transaction_manager.as_ref() {
            let _ = transaction_manager.commit(self.transaction.as_ref().unwrap());
        }
    }
}

#[cfg(test)]
mod test {
    use crate::memtables::memtable::MemTable;
    use crate::memtables::memtable_iterator::MemtableIterator;
    use crate::transactions::transaction::Transaction;
    use crate::utils::storage_engine_iterator::StorageEngineIterator;
    use bytes::Bytes;
    use shared::iterators::storage_iterator::StorageIterator;
    use shared::key::Key;
    use shared::StorageValueMergeResult;
    use std::sync::Arc;

    #[test]
    fn iterator_one_entry() {
        let options = shared::start_simpledb_options_builder_from(&shared::SimpleDbOptions::default())
            .storage_value_merger(|a, b, _| merge_values(a, b))
            .build_arc();
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0, 0)
            .unwrap());
        memtable.set(&transaction(10), Bytes::from("aa"), &vec![1]);

        let mut iterator = StorageEngineIterator::create(
            0,
            &options,
            MemtableIterator::create(&memtable, &Transaction::none()),
        );

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("aa", 10)));
        assert!(!iterator.next());
    }

    #[test]
    fn iterator_empty() {
        let options = Arc::new(shared::SimpleDbOptions::default());
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0, 0)
            .unwrap());

        let mut iterator = StorageEngineIterator::create(
            0,
            &options,
            MemtableIterator::create(&memtable, &Transaction::none()),
        );

        assert!(!iterator.next());
    }

    #[test]
    fn iterator_no_merger_fn() {
        let options = Arc::new(shared::SimpleDbOptions::default());
        let memtable = Arc::new(MemTable::create_mock(Arc::new(shared::SimpleDbOptions::default()), 0, 0)
            .unwrap());
        memtable.set(&transaction(10), Bytes::from("aa"), &vec![1]);
        memtable.set(&transaction(1), Bytes::from("alberto"), &vec![2]);
        memtable.set(&transaction(3), Bytes::from("alberto"), &vec![4]);
        memtable.set(&transaction(1), Bytes::from("gonchi"), &vec![5]);
        memtable.set(&transaction(5), Bytes::from("javier"), &vec![6]);
        memtable.set(&transaction(5), Bytes::from("jaime"), &vec![8]);
        memtable.set(&transaction(1), Bytes::from("wili"), &vec![9]);

        let mut iterator = StorageEngineIterator::create(
            0,
            &options,
            MemtableIterator::create(&memtable, &Transaction::none()),
        );

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("aa", 10)));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("alberto", 1)));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("alberto", 3)));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("gonchi", 1)));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("jaime", 5)));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("javier", 5)));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("wili", 1)));

        assert!(!iterator.next());
    }

    fn merge_values(a: &Bytes, b: &Bytes) -> StorageValueMergeResult {
        let a_vec = a.to_vec();
        let b_vec = b.to_vec();

        if b_vec[0] == 10 {
            StorageValueMergeResult::DiscardPreviousKeepNew
        } else if a_vec[0] == 10 {
            StorageValueMergeResult::Ok(b.clone())
        } else {
            StorageValueMergeResult::Ok(Bytes::from(vec![a_vec[0] + b_vec[0]]))
        }
    }

    #[test]
    fn iterator_merger_fn() {
        let options = shared::start_simpledb_options_builder_from(&shared::SimpleDbOptions::default())
            .storage_value_merger(|a, b, _| merge_values(a, b))
            .build_arc();

        let memtable = Arc::new(MemTable::create_mock(options.clone(), 0, 0).unwrap());
        memtable.set(&transaction(10), Bytes::from("aa"), &vec![1]);
        memtable.set(&transaction(1), Bytes::from("alberto"), &vec![1]);
        memtable.set(&transaction(3), Bytes::from("alberto"), &vec![1]);
        memtable.set(&transaction(4), Bytes::from("alberto"), &vec![1]);
        memtable.set(&transaction(1), Bytes::from("gonchi"), &vec![1]);
        memtable.set(&transaction(5), Bytes::from("javier"), &vec![1]);
        memtable.set(&transaction(5), Bytes::from("jaime"), &vec![1]);
        memtable.set(&transaction(1), Bytes::from("wili"), &vec![1]);
        memtable.set(&transaction(1), Bytes::from("wili"), &vec![10]); //10 Equivalent of tombstone
        memtable.set(&transaction(1), Bytes::from("wili"), &vec![2]);

        let mut iterator = StorageEngineIterator::create(
            0,
            &options,
            MemtableIterator::create(&memtable, &Transaction::none()),
        );

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("aa", 10)));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("alberto", 4)));
        assert!(iterator.value().eq(&vec![3]));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("gonchi", 1)));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("jaime", 5)));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("javier", 5)));

        assert!(iterator.next());
        assert!(iterator.key().eq(&Key::create_from_str("wili", 1)));
        assert!(iterator.value().eq(&vec![2]));

        assert!(!iterator.next());
    }

    fn transaction(txn_id: shared::TxnId) -> Transaction {
        let mut transaction = Transaction::none();
        transaction.txn_id = txn_id;
        transaction
    }
}