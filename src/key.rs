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