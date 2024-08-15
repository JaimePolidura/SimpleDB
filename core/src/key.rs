use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use crate::key;

#[derive(Debug)]
pub struct Key {
    string: String,
    timestamp: u64,
}

pub fn new(string: &str, timestamp: u64) -> Key {
    Key {
        string: string.to_string(),
        timestamp
    }
}

impl Key {
    pub fn len(&self) -> usize {
        self.string.len()
    }

    pub fn is_empty(&self) -> bool {
        self.string.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.string.as_bytes()
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    //"Juan".prefix_difference("Justo") -> (2, 2)
    pub fn prefix_difference(&self, other: &Key) -> (usize, usize) {
        let mut same_chars_count = 0;
        let mut current_char_self = self.string.chars();
        let mut current_char_other = other.string.chars();

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
        let (h1, h2) = self.string.split_at(index);
        (key::new(h1, self.timestamp), key::new(h2, self.timestamp))
    }

    pub fn merge(a: &Key, b: &Key, timestamp: u64) -> Key {
        let mut result = String::from(&a.string);
        result.extend(b.string.chars());
        Key {string: result, timestamp }
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.string)
    }
}

impl Default for Key {
    fn default() -> Self {
        Key{ string: String::from(""), timestamp: 0 }
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.string.eq(&other.string) && self.timestamp == other.timestamp
    }
}

impl Eq for Key {}

impl Clone for Key {
    fn clone(&self) -> Self {
        let cloned = self.string.clone();
        Key { string: cloned, timestamp: self.timestamp }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.string.partial_cmp(&other.string) {
            Some(Ordering::Equal) => self.timestamp.partial_cmp(&other.timestamp),
            other => other,
        }
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.string.cmp(&other.string) {
            Ordering::Equal => self.timestamp.cmp(&other.timestamp),
            other => other,
        }
    }
}