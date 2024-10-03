use crate::index::posting_list_iterator::PostingListIterator;
use storage::key::Key;
use storage::transactions::transaction::Transaction;
use storage::SimpleDbStorageIterator;

//This iterator will return the primary keys indexed::
//  - These primary keys are readable by the transaction
//  - They are not deleted
pub struct SecondaryIndexIterator {
    iterator: SimpleDbStorageIterator,
    transaction: Transaction,

    current_inner_iterator: Option<PostingListIterator>
}

impl SecondaryIndexIterator {
    pub fn create(
        transaction: &Transaction,
        iterator: SimpleDbStorageIterator,
    ) -> SecondaryIndexIterator {
        SecondaryIndexIterator {
            transaction: transaction.clone(),
            current_inner_iterator: None,
            iterator
        }
    }

    pub fn next(&mut self) -> Option<Key> {
        loop {

        }
    }
}