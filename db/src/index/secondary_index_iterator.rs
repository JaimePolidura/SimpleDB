use crate::index::posting_list::PostingList;
use crate::index::posting_list_iterator::PostingListIterator;
use bytes::Bytes;
use crossbeam_skiplist::SkipSet;
use shared::iterators::storage_iterator::StorageIterator;
use shared::key::Key;
use shared::{TxnId, Type};
use storage::transactions::transaction::Transaction;

//This iterator will return the primary keys indexed::
//  - These primary keys are readable by the transaction
//  - They are not deleted
pub struct SecondaryIndexIterator<I: StorageIterator> {
    transaction: Transaction,

    primary_column_type: Type,

    posting_list_iterator: Option<PostingListIterator>,
    storage_iterator: I,

    deleted_entries: SkipSet<TxnId>,
}

impl<I: StorageIterator> SecondaryIndexIterator<I> {
    pub fn create(
        transaction: &Transaction,
        iterator: I,
        primary_column_type: Type
    ) -> SecondaryIndexIterator<I> {
        SecondaryIndexIterator {
            transaction: transaction.clone(),
            deleted_entries: SkipSet::new(),
            posting_list_iterator: None,
            storage_iterator: iterator,
            primary_column_type
        }
    }

    //Returns (secondary indexed value, primary key)
    pub fn next(&mut self) -> Option<(Key, Key)> {
        loop {
            if !self.go_to_next() {
                return None;
            }

            let (next_entry, secondary_value) = self.posting_list_iterator
                .as_mut()
                .unwrap()
                .next()
                .unwrap();

            if !next_entry.is_present {
                self.deleted_entries.insert(next_entry.primary_key.txn_id());
            } else {
                return Some((secondary_value, next_entry.primary_key.clone()));
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
        let posting_list_secondary_value = self.storage_iterator.key();

        let posting_list = PostingList::deserialize(&mut posting_list_bytes.clone(), self.primary_column_type);
        self.posting_list_iterator = Some(PostingListIterator::create(
            posting_list_secondary_value.clone(), &self.transaction, posting_list
        ));

        true
    }

    pub fn seek(&mut self, key: &Bytes, inclusive: bool) {
        self.storage_iterator.seek(key, inclusive);
    }
}

#[cfg(test)]
mod test  {
    use crate::index::posting_list::PostingList;
    use crate::index::secondary_index_iterator::SecondaryIndexIterator;
    use bytes::Bytes;
    use shared::iterators::mock_iterator::MockIterator;
    use shared::key::Key;
    use shared::Type;
    use storage::transactions::transaction::Transaction;

    #[test]
    fn iterator_empty() {
        let mut secondary_index_iterator = SecondaryIndexIterator::create(
            &Transaction::none(),
            MockIterator::create(),
            Type::String,
        );
        secondary_index_iterator.seek(&Bytes::from("A".as_bytes()), true);

        assert!(secondary_index_iterator.next().is_none());
    }

    /**
    primary key -> [ ( key, txnid, is_present ) ]
    1 -> [ (Jaime, 1, true), (Molon, 2, true), (Wili, 3, false) ]
    2 -> [ (Wili, 3, true), (Walo, 2, true) ]
    3 -> [ (Juanxli, 10, true), (Alvaro, 2, false) ]
    */
    #[test]
    fn iterator_seek() {
        let mut secondary_index_iterator = create_secondary_index_iterator();
        secondary_index_iterator.seek(&Bytes::from("2".as_bytes().to_vec()), true);

        assert_eq!(secondary_index_iterator.next(), Some((Key::create_from_str("2", 1), Key::create_from_str("Wili", 4))));
        assert_eq!(secondary_index_iterator.next(), Some((Key::create_from_str("2", 1), Key::create_from_str("Walo", 2))));
        assert_eq!(secondary_index_iterator.next(), Some((Key::create_from_str("3", 1), Key::create_from_str("Alvaro", 2))));
        assert_eq!(secondary_index_iterator.next(), None);
    }

    /**
    primary key -> [ ( key, txnid, is_present ) ]
    1 -> [ (Jaime, 1, true), (Molon, 2, true), (Wili, 3, false) ]
    2 -> [ (Wili, 3, true), (Walo, 2, true) ]
    3 -> [ (Juanxli, 10, true), (Alvaro, 2, false) ]
    */
    #[test]
    fn iterator() {
        let mut secondary_index_iterator = create_secondary_index_iterator();

        assert_eq!(secondary_index_iterator.next(), Some((Key::create_from_str("1", 1), Key::create_from_str("Jaime", 1))));
        assert_eq!(secondary_index_iterator.next(), Some((Key::create_from_str("1", 1), Key::create_from_str("Molon", 2))));
        assert_eq!(secondary_index_iterator.next(), Some((Key::create_from_str("2", 1), Key::create_from_str("Wili", 4))));
        assert_eq!(secondary_index_iterator.next(), Some((Key::create_from_str("2", 1), Key::create_from_str("Walo", 2))));
        assert_eq!(secondary_index_iterator.next(), Some((Key::create_from_str("3", 1), Key::create_from_str("Alvaro", 2))));
        assert_eq!(secondary_index_iterator.next(), None);
    }

    fn create_secondary_index_iterator() -> SecondaryIndexIterator<MockIterator> {
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

        SecondaryIndexIterator::create(
            &Transaction::create(5),
            inner_iterator,
            Type::I64,
        )
    }
}