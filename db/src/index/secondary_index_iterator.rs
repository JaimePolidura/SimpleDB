use crate::index::posting_list::PostingList;
use crate::index::posting_list_iterator::PostingListIterator;
use crossbeam_skiplist::SkipSet;
use shared::TxnId;
use storage::key::Key;
use storage::transactions::transaction::Transaction;
use storage::utils::storage_iterator::StorageIterator;

//This iterator will return the primary keys indexed::
//  - These primary keys are readable by the transaction
//  - They are not deleted
pub struct SecondaryIndexIterator<I: StorageIterator> {
    transaction: Transaction,

    posting_list_iterator: Option<PostingListIterator>,
    storage_iterator: I,

    deleted_entries: SkipSet<TxnId>,
}

impl<I: StorageIterator> SecondaryIndexIterator<I> {
    pub fn create(
        transaction: &Transaction,
        iterator: I,
    ) -> SecondaryIndexIterator<I> {
        SecondaryIndexIterator {
            transaction: transaction.clone(),
            deleted_entries: SkipSet::new(),
            posting_list_iterator: None,
            storage_iterator: iterator
        }
    }

    pub fn next(&mut self) -> Option<Key> {
        loop {
            if !self.go_to_next() {
                return None;
            }

            let next_entry = self.posting_list_iterator
                .as_mut()
                .unwrap()
                .next()
                .unwrap();

            if !next_entry.is_present {
                self.deleted_entries.insert(next_entry.primary_key.txn_id());
            } else {
                return Some(next_entry.primary_key.clone());
            }
        }
    }

    //This function will set posting_list_iterator to point to the next available entry in the iterator
    //It return false, if there are no more entries to return
    fn go_to_next(&mut self) -> bool {
        loop {
            match &mut self.posting_list_iterator {
                Some(posting_list_iterator) => {
                    if posting_list_iterator.has_next() {
                        return true;
                    }
                    if !self.next_posting_list_iterator() {
                        return false;
                    }
                }
                None => {
                    if !self.next_posting_list_iterator() {
                        return false;
                    }
                }
            };
        }
    }

    fn next_posting_list_iterator(&mut self) -> bool {
        if !self.storage_iterator.next() {
            return false;
        }

        let posting_list_bytes = self.storage_iterator.value();
        let posting_list = PostingList::deserialize(&mut posting_list_bytes.clone());
        self.posting_list_iterator = Some(PostingListIterator::create(&self.transaction, posting_list));
        true
    }
}

#[cfg(test)]
mod test  {
    use crate::index::posting_list::PostingList;
    use crate::index::secondary_index_iterator::SecondaryIndexIterator;
    use bytes::Bytes;
    use shared::SimpleDbOptions;
    use std::sync::Arc;
    use storage::transactions::transaction::Transaction;
    use storage::utils::storage_engine_iterator::StorageEngineIterator;
    use storage::key;

    /**
    primary key -> [ ( key, txnid, is_present ) ]
    1 -> [ (Jaime, 1, true), (Molon, 2, true), (Wili, 3, false) ]
    2 -> [ (Wili, 3, true), (Walo, 2, true) ]
    3 -> [ (Juanxli, 10, true), (Alvaro, 2, false) ]
    */
    #[test]
    fn test() {
        let mut inner_iterator = storage::MockIterator::create();
        inner_iterator.add_entry("1", 1, Bytes::from(PostingList::create(vec![
            ("Jaime", 1, true),
            ("Molon", 2, true),
            ("Wili", 3, false)
        ]).serialize()));
        inner_iterator.add_entry("2", 1, Bytes::from(PostingList::create(vec![
            ("Wili", 4, true),
            ("Walo", 2, true)
        ]).serialize()));
        inner_iterator.add_entry("3", 1, Bytes::from(PostingList::create(vec![
            ("Juanxli", 10, true),
            ("Alvaro", 2, true)
        ]).serialize()));

        let iterator = StorageEngineIterator::create(0, &Arc::new(SimpleDbOptions::default()), inner_iterator);
        let mut secondary_index_iterator = SecondaryIndexIterator::create(
            &Transaction::create(5),
            iterator
        );

        assert_eq!(secondary_index_iterator.next(), Some(key::create_from_str("Jaime", 1)));
        assert_eq!(secondary_index_iterator.next(), Some(key::create_from_str("Molon", 2)));
        assert_eq!(secondary_index_iterator.next(), Some(key::create_from_str("Wili", 4)));
        assert_eq!(secondary_index_iterator.next(), Some(key::create_from_str("Walo", 2)));
        assert_eq!(secondary_index_iterator.next(), Some(key::create_from_str("Alvaro", 2)));
        assert_eq!(secondary_index_iterator.next(), None);
    }
}