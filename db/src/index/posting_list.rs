use bytes::{Buf, BufMut, Bytes};
use shared::key::Key;
use shared::{KeyspaceId, TxnId, Type};
use storage::transactions::transaction::Transaction;

#[derive(Clone, Debug, PartialOrd, PartialEq)]
pub struct PostingList {
    pub(crate) entries: Vec<PostingListEntry>,
}

#[derive(Clone, Debug, PartialOrd, PartialEq)]
pub struct PostingListEntry {
    pub(crate) is_present: bool,
    pub(crate) primary_key: Key,
}

impl PostingList {
    pub fn create_empty() -> PostingList {
        PostingList { entries: Vec::new() }
    }

    pub fn create_deleted(
        id: Bytes,
        primary_key_type: Type,
        transaction: &Transaction
    ) -> PostingList {
        PostingList {
            entries: vec![PostingListEntry{
                primary_key: Key::create(id,  primary_key_type, transaction.id()),
                is_present: false,
            }]
        }
    }

    pub fn crate_only_one_entry(primary_key: &Key) -> PostingList {
        let mut entries = Vec::new();
        entries.push(PostingListEntry {
            primary_key: primary_key.clone(),
            is_present: true,
        });

        PostingList { entries }
    }

    //Used for testing
    pub fn create_mock(
        values: Vec<(&str, TxnId, bool)>
    ) -> PostingList {
        let mut entries = Vec::new();
        for value in values {
            entries.push(PostingListEntry{
                primary_key: Key::create(Bytes::copy_from_slice(value.0.as_bytes()), Type::String, value.1),
                is_present: value.2
            });
        }

        PostingList { entries }
    }

    //A is after B
    pub fn merge(a: &PostingList, b: &PostingList) -> PostingList {
        let mut final_posting_list = PostingList::create_empty();

        for current_entry_a in &a.entries {
            if current_entry_a.is_present {
                final_posting_list.entries.push(current_entry_a.clone());
            }
        }

        for current_entry_b in &b.entries {
            match final_posting_list.get_entry_by_key_bytes(&current_entry_b.primary_key) {
                None => {
                    if current_entry_b.is_present {
                        final_posting_list.entries.push(current_entry_b.clone());
                    }
                },
                _ => {}
            };
        }

        final_posting_list
    }

    pub fn is_emtpy(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn deserialize(ptr: &mut &[u8], primary_key_type: Type) -> PostingList {
        let n_entries = ptr.get_u64_le() as usize;
        let mut entries = Vec::with_capacity(n_entries);

        for _ in 0..n_entries {
            let is_present = ptr.get_u8() != 0x00;
            let primary_key = Key::deserialize(ptr, primary_key_type);

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

    fn get_entry_by_key_bytes(&self, key: &Key) -> Option<&PostingListEntry> {
        for self_entry in &self.entries {
            if self_entry.primary_key.bytes_eq(key) {
                return Some(self_entry);
            }
        }

        None
    }
}

#[cfg(test)]
mod test {
    use shared::key::Key;
    use shared::Type;
    use crate::index::posting_list::{PostingList, PostingListEntry};

    #[test]
    fn serialize_deserialize() {
        let posting_list = PostingList::create_mock(vec![
            ("Jaime", 1, true),
            ("Juan", 2, false),
            ("Walo", 1, true),
        ]);
        let serialized = posting_list.serialize();

        let deserialized = PostingList::deserialize(&mut serialized.as_slice(), Type::String);

        assert_eq!(deserialized, PostingList{entries: vec![
            PostingListEntry{ primary_key: Key::create_from_str("Jaime", 1), is_present: true },
            PostingListEntry{ primary_key: Key::create_from_str("Juan", 2), is_present: false },
            PostingListEntry{ primary_key: Key::create_from_str("Walo", 1), is_present: true },
        ]});
    }

    #[test]
    fn merge() {
        let posting_list_a = PostingList::create_mock(vec![
            ("Jaime", 1, true),
            ("Juan", 2, false),
            ("Walo", 1, true),
        ]);
        let posting_list_b = PostingList::create_mock(vec![
            ("Juan", 3, true),
            ("Pedro", 1, true),
        ]);

        let merge_result = PostingList::merge(&posting_list_a, &posting_list_b);

        assert_eq!(merge_result, PostingList{entries: vec![
            PostingListEntry{ primary_key: Key::create_from_str("Jaime", 1), is_present: true },
            PostingListEntry{ primary_key: Key::create_from_str("Walo", 1), is_present: true },
            PostingListEntry{ primary_key: Key::create_from_str("Juan", 3), is_present: true },
            PostingListEntry{ primary_key: Key::create_from_str("Pedro", 1), is_present: true },
        ]});
    }
}