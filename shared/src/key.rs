use crate::{key, TxnId};
use bytes::{Buf, BufMut, Bytes};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug)]
pub struct Key {
    bytes: Bytes,
    txn_id: TxnId,
}

impl Key {
    pub fn create_from_str(string: &str, txn_id: TxnId) -> Key {
        Key {
            bytes: Bytes::from(string.to_string()),
            txn_id
        }
    }

    pub fn create(bytes: Bytes, txn_id: TxnId) -> Key {
        Key {
            bytes,
            txn_id
        }
    }

    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    pub fn as_bytes(&self) -> &Bytes {
        &self.bytes
    }

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    pub fn bytes_eq_bytes(&self, other: &Bytes) -> bool {
        self.bytes.eq(other)
    }

    pub fn bytes_gt_bytes(&self, other: &Bytes) -> bool {
        self.bytes.gt(other)
    }

    pub fn bytes_ge_bytes(&self, other: &Bytes) -> bool {
        self.bytes.ge(other)
    }

    pub fn bytes_lt_bytes(&self, other: &Bytes) -> bool {
        self.bytes.lt(other)
    }

    pub fn bytes_le_bytes(&self, other: &Bytes) -> bool {
        self.bytes.le(other)
    }

    pub fn bytes_eq(&self, other: &Key) -> bool {
        self.bytes == other.bytes
    }

    pub fn serialized_key_size(ptr: &mut &[u8]) -> usize {
        let _ = ptr.get_u64_le() as TxnId;
        let bytes_len = ptr.get_u16_le();

        8 + 2 + bytes_len as usize
    }

    pub fn deserialize(ptr: &mut &[u8]) -> Key {
        let txn_id = ptr.get_u64_le() as TxnId;
        let bytes_len = ptr.get_u16_le();
        let bytes = &ptr[.. bytes_len as usize];
        ptr.advance(bytes_len as usize);

        Key {
            bytes: Bytes::copy_from_slice(bytes),
            txn_id,
        }
    }

    pub fn serialized_size(&self) -> usize {
        8 + 2 + self.bytes.len()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.put_u64_le(self.txn_id as u64);
        serialized.put_u16_le(self.bytes.len() as u16);
        serialized.extend(self.bytes.as_ref());
        serialized
    }

    //"Juan".prefix_difference("Justo") -> (2, 2)
    pub fn prefix_difference(&self, other: &Key) -> (usize, usize) {
        let mut same_chars_count = 0;
        let mut current_char_self = self.bytes.iter();
        let mut current_char_other = other.bytes.iter();

        while let (
            Some(char_self), Some(char_other)) =
            (current_char_self.next(), current_char_other.next()) {

            if char_self != char_other {
                break
            }

            same_chars_count = same_chars_count + 1;
        }

        (same_chars_count, self.len() - same_chars_count)
    }

    //"Juan".split(2) -> ("Ju", "an")
    pub fn split(&self, index: usize) -> (Key, Key) {
        let (h1, h2) = self.bytes.split_at(index);
        (Key::create(Bytes::from(h1.to_vec()), self.txn_id), Key::create(Bytes::from(h2.to_vec()), self.txn_id))
    }

    pub fn merge(a: &Key, b: &Key, txn_id: TxnId) -> Key {
        let mut result = Vec::from(a.bytes.as_ref());
        result.extend(b.bytes.as_ref());
        Key { bytes: Bytes::from(result), txn_id }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match String::from_utf8(self.bytes.as_ref().to_vec()) {
            Ok(string) => write!(f, "{}", string),
            Err(_) => write!(f, "Key cannot be converted to UTF-8 string")
        }
    }
}

impl Default for Key {
    fn default() -> Self {
        Key{ bytes: Bytes::from(vec![]), txn_id: 0 }
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.bytes.eq(&other.bytes) && self.txn_id == other.txn_id
    }
}

impl Eq for Key {}

impl Clone for Key {
    fn clone(&self) -> Self {
        let cloned = self.bytes.clone();
        Key { bytes: cloned, txn_id: self.txn_id }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.bytes.partial_cmp(&other.bytes) {
            Some(Ordering::Equal) => self.txn_id.partial_cmp(&other.txn_id),
            other => other,
        }
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.bytes.cmp(&other.bytes) {
            Ordering::Equal => self.txn_id.cmp(&other.txn_id),
            other => other,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::key::Key;

    #[test]
    fn serialize_deserialize() {
        let key = Key::create_from_str("Jaime", 1);
        let serialized = key.serialize();
        let deserialized = Key::deserialize(&mut serialized.as_slice());

        assert_eq!(deserialized, Key::create_from_str("Jaime", 1));
    }
}