use crossbeam_skiplist::SkipSet;
use shared::TxnId;
use crate::index::posting_list::PostingList;
use crate::index::posting_list_iterator::PostingListIterator;
use storage::key::Key;
use storage::transactions::transaction::Transaction;
use storage::utils::storage_iterator::StorageIterator;
use storage::SimpleDbStorageIterator;

//This iterator will return the primary keys indexed::
//  - These primary keys are readable by the transaction
//  - They are not deleted
pub struct SecondaryIndexIterator {
    transaction: Transaction,

    posting_list_iterator: Option<PostingListIterator>,
    storage_iterator: SimpleDbStorageIterator,

    deleted_entries: SkipSet<TxnId>,
}

impl SecondaryIndexIterator {
    pub fn create(
        transaction: &Transaction,
        iterator: SimpleDbStorageIterator,
    ) -> SecondaryIndexIterator {
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
        return true;
    }
}