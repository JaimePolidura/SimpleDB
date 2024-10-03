use bytes::{Buf, BufMut, Bytes};
use shared::SimpleDbError::OnlyOnePrimaryColumnAllowed;
use shared::StorageValueMergeResult;
use storage::key;
use storage::key::Key;
use storage::transactions::transaction::Transaction;

#[derive(Clone)]
pub struct PostingList {
    entries: Vec<PostingListEntry>,
}

#[derive(Clone)]
pub struct PostingListEntry {
    is_present: bool,
    primary_key: Key
}

impl PostingList {
    pub fn create_empty() -> PostingList {
        PostingList { entries: Vec::new() }
    }

    pub fn create_new(
        id: Bytes,
        transaction: &Transaction
    ) -> PostingList {
        PostingList {
            entries: vec![PostingListEntry{
                primary_key: key::create(id, transaction.id()),
                is_present: true,
            }]
        }
    }

    pub fn create_deleted(
        id: Bytes,
        transaction: &Transaction
    ) -> PostingList {
        PostingList {
            entries: vec![PostingListEntry{
                primary_key: key::create(id, transaction.id()),
                is_present: false,
            }]
        }
    }

    //A is after B
    pub fn merge(a: &PostingList, b: &PostingList) -> PostingList {
        let mut final_posting_list = PostingList::create_empty();

        for current_entry_a in &a.entries {
            match b.get_entry_by_key_bytes(&current_entry_a.primary_key) {
                Some(_) => {
                    if current_entry_a.is_present {
                        final_posting_list.entries.push(current_entry_a.clone())
                    }
                },
                None => final_posting_list.entries.push(current_entry_a.clone()),
            };
        }

        for current_entry_b in &b.entries {
            match a.get_entry_by_key_bytes(&current_entry_b.primary_key) {
                None => final_posting_list.entries.push(current_entry_b.clone()),
                _ => {}
            };
        }

        final_posting_list
    }

    pub fn is_emtpy(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get_entry_by_key_bytes(&self, key: &Key) -> Option<&PostingListEntry> {
        for self_entry in &self.entries {
            if self_entry.primary_key.bytes_eq(key) {
                return Some(self_entry);
            }
        }

        None
    }

    pub fn deserialize(ptr: &mut &[u8]) -> PostingList {
        let n_entries = ptr.get_u64_le() as usize;
        let mut entries = Vec::with_capacity(n_entries);

        for _ in 0..n_entries {
            let is_present = ptr.get_u8() != 0x00;
            let primary_key = Key::deserialize(ptr);

            entries.push(PostingListEntry {is_present, primary_key})
        }

        PostingList { entries }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.put_u64_le(self.entries.len() as u64);

        for entry in &self.entries {
            serialized.put_u8(if entry.is_present { 0x01 } else { 0x00 });
            serialized.extend(entry.primary_key.serialize());
        }

        serialized
    }
}