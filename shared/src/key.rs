use crate::{TxnId, Type, Value};
use bytes::{Buf, BufMut, Bytes};
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;

#[derive(Debug)]
pub struct Key {
    value: Value,
    txn_id: TxnId,
}

impl Key {
    pub fn create_from_str(string: &str, txn_id: TxnId) -> Key {
        Key {
            value: Value::create(Bytes::copy_from_slice(string.as_bytes()), Type::String).unwrap(),
            txn_id
        }
    }

    pub fn create(value_bytes: Bytes, value_type: Type, txn_id: TxnId) -> Key {
        Key {
            value: Value::create(value_bytes, value_type).unwrap(),
            txn_id
        }
    }

    pub fn get_type(&self) -> Type {
        self.value.get_type()
    }

    pub fn len(&self) -> usize {
        self.value.get_bytes().len()
    }

    pub fn is_empty(&self) -> bool {
        self.value.get_bytes().is_empty()
    }

    pub fn as_bytes(&self) -> &Bytes {
        self.value.get_bytes()
    }

    pub fn txn_id(&self) -> TxnId {
        self.txn_id
    }

    pub fn bytes_eq_bytes(&self, other: &Bytes) -> bool {
        self.value.get_bytes().eq(other)
    }

    pub fn bytes_gt_bytes(&self, other: &Bytes) -> bool {
        self.value.gt_bytes(other)
    }

    pub fn bytes_ge_bytes(&self, other: &Bytes) -> bool {
        self.value.ge_bytes(other)
    }

    pub fn bytes_lt_bytes(&self, other: &Bytes) -> bool {
        self.value.lt_bytes(other)
    }

    pub fn bytes_le_bytes(&self, other: &Bytes) -> bool {
        self.value.le_bytes(other)
    }

    pub fn bytes_eq(&self, other: &Key) -> bool {
        self.value == other.value
    }

    pub fn serialized_key_size(ptr: &mut &[u8]) -> usize {
        let _ = ptr.get_u64_le() as TxnId;
        let bytes_len = ptr.get_u16_le();

        8 + 2 + bytes_len as usize
    }

    pub fn deserialize(ptr: &mut &[u8], value_type: Type) -> Key {
        let txn_id = ptr.get_u64_le() as TxnId;
        let bytes_len = ptr.get_u16_le();
        let bytes = &ptr[.. bytes_len as usize];
        ptr.advance(bytes_len as usize);

        Key {
            value: Value::create(Bytes::copy_from_slice(bytes), value_type).unwrap(),
            txn_id,
        }
    }

    pub fn serialized_size(&self) -> usize {
        8 + 2 + self.value.get_bytes().len()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.put_u64_le(self.txn_id as u64);
        serialized.put_u16_le(self.len() as u16);
        serialized.extend(self.value.get_bytes());
        serialized
    }

    //"Juan".prefix_difference("Justo") -> (2, 2)
    pub fn prefix_difference(&self, other: &Key) -> (usize, usize) {
        let mut same_chars_count = 0;
        let mut current_char_other = other.value.get_bytes().iter();
        let mut current_char_self = self.value.get_bytes().iter();

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
        let (h1, h2) = self.value.get_bytes().split_at(index);
        (Key::create(Bytes::from(h1.to_vec()), self.value.get_type(), self.txn_id),
         Key::create(Bytes::from(h2.to_vec()), self.value.get_type(), self.txn_id))
    }

    pub fn merge(a: &Key, b: &Key, txn_id: TxnId) -> Key {
        let mut result = a.value.get_bytes().to_vec();
        result.extend(b.value.get_bytes());
        Key { value: Value::create(Bytes::from(result), a.value.get_type()).unwrap(), txn_id }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match String::from_utf8(self.value.get_bytes().to_vec()) {
            Ok(string) => write!(f, "{}", string),
            Err(_) => write!(f, "Key cannot be converted to UTF-8 string")
        }
    }
}

impl Default for Key {
    fn default() -> Self {
        Key{ value: Value::create_null(), txn_id: 0 }
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value) && self.txn_id == other.txn_id
    }
}

impl Eq for Key {}

impl Clone for Key {
    fn clone(&self) -> Self {
        let cloned = self.value.clone();
        Key { value: cloned, txn_id: self.txn_id }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.value.partial_cmp(&other.value) {
            Some(Ordering::Equal) => self.txn_id.partial_cmp(&other.txn_id),
            other => other,
        }
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.value.cmp(&other.value) {
            Ordering::Equal => self.txn_id.cmp(&other.txn_id),
            other => other,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::key::Key;
    use crate::Type;

    #[test]
    fn serialize_deserialize() {
        let key = Key::create_from_str("Jaime", 1);
        let serialized = key.serialize();
        let deserialized = Key::deserialize(&mut serialized.as_slice(), Type::String);

        assert_eq!(deserialized, Key::create_from_str("Jaime", 1));
    }
}