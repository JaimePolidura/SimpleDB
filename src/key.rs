use std::fmt;
use std::fmt::Formatter;

#[derive(Debug)]
pub struct Key {
    string: String,
}

impl Key {
    pub fn len(&self) -> usize {
        self.string.len()
    }

    pub fn is_empty(&self) -> bool {
        self.string.is_empty()
    }

    pub fn new(string: &str) -> Key {
        Key {
            string: string.to_string()
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.string.as_bytes()
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
        (Key::new(h1), Key::new(h2))
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.string)
    }
}

impl Default for Key {
    fn default() -> Self {
        Key{ string: String::from("") }
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.string.eq(&other.string)
    }
}

impl Eq for Key {}

impl Clone for Key {
    fn clone(&self) -> Self {
        Key { string: self.string.clone() }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.string.partial_cmp(&other.string)
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.string.cmp(&other.string)
    }
}