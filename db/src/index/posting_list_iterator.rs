use bytes::Bytes;
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

    pub fn has_next(&self) -> bool {
        if self.posting_list.is_empty() {
            return false;
        }

        for posting_list in &self.posting_list {
            if self.transaction.can_read(&posting_list.primary_key) {
                return true;
            }
        }

        false
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

#[cfg(test)]
mod test {
    use storage::key;
    use storage::transactions::transaction::Transaction;
    use crate::index::posting_list::{PostingList, PostingListEntry};
    use crate::index::posting_list_iterator::PostingListIterator;

    #[test]
    fn iterator() {
        let mut posting_list = PostingList::create_empty();
        posting_list.entries.push(PostingListEntry{ is_present: true, primary_key: key::create_from_str("a", 1) });
        posting_list.entries.push(PostingListEntry{ is_present: true, primary_key: key::create_from_str("b", 2) });
        posting_list.entries.push(PostingListEntry{ is_present: true, primary_key: key::create_from_str("c", 3) });
        posting_list.entries.push(PostingListEntry{ is_present: false, primary_key: key::create_from_str("d", 8) });
        posting_list.entries.push(PostingListEntry{ is_present: true, primary_key: key::create_from_str("f", 2) });
        posting_list.entries.push(PostingListEntry{ is_present: true, primary_key: key::create_from_str("g", 8) });


        let mut iterator = PostingListIterator::create(&Transaction::create(4), posting_list);
        assert!(iterator.has_next());

        assert_eq!(iterator.next(), Some(PostingListEntry{ is_present: true, primary_key: key::create_from_str("a", 1) }));
        assert!(iterator.has_next());

        assert_eq!(iterator.next(), Some(PostingListEntry{ is_present: true, primary_key: key::create_from_str("b", 2) }));
        assert!(iterator.has_next());

        assert_eq!(iterator.next(), Some(PostingListEntry{ is_present: true, primary_key: key::create_from_str("c", 3) }));
        assert!(iterator.has_next());

        assert_eq!(iterator.next(), Some(PostingListEntry{ is_present: true, primary_key: key::create_from_str("f", 2) }));
        assert!(!iterator.has_next());
    }
}