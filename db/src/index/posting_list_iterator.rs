use storage::transactions::transaction::Transaction;
use crate::index::posting_list::{PostingList, PostingListEntry};

//This iterator:
// - Will only return posting list entries that are readable by the current transaction
pub struct PostingListIterator {
    transaction: Transaction,
    posting_list: Vec<PostingListEntry>,
}

impl PostingListIterator {
    pub fn create(
        transaction: &Transaction,
        posting_list: PostingList,
    ) -> PostingListIterator {
        PostingListIterator {
            posting_list: posting_list.entries,
            transaction: transaction.clone(),
        }
    }

    pub fn next(&mut self) -> Option<PostingListEntry> {
        loop {
            if self.posting_list.is_empty() {
                return None;
            }

            let posting_list = self.posting_list.remove(0);
            if self.transaction.can_read(&posting_list.primary_key) {
                return Some(posting_list);
            }
        }
    }
}