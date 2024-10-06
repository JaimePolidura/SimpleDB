use crate::iterators::storage_iterator::StorageIterator;
use crate::key::Key;

pub fn assert_empty_iterator<I>(
    mut iterator: I
)
where
    I: StorageIterator
{
    assert!(!iterator.has_next());
    assert!(!iterator.next());
    assert!(!iterator.has_next());
}

pub fn assert_iterator_str_seq<I>(
    iterator: I,
    mut expected_strs: Vec<&str>
)
where
    I: StorageIterator
{
    let mut keys = Vec::new();
    for expected_str in expected_strs {
        keys.push(Key::create_from_str(expected_str, 0));
    }
    assert_iterator_key_seq(iterator, keys)
}

pub fn assert_iterator_key_seq<I>(
    mut iterator: I,
    mut expected_keys: Vec<Key>
)
where
    I: StorageIterator
{
    let initial_expected_n_elements = expected_keys.len();
    let mut current_index = -1;

    while iterator.next() && !expected_keys.is_empty() {
        let actual = iterator.key();
        let expected = expected_keys.remove(0);
        current_index += 1;

        if *actual != expected {
            assert_eq!(*actual, expected, "{}", format!("Expected key: {:?} Actual: {:?} Index: {}",
                                                       expected.to_string(), *actual, current_index.to_string()));
        }
        if !expected_keys.is_empty() {
            assert!(iterator.has_next());
        }
    }

    if iterator.has_next() {
        panic!("Iterator still have values")
    }
    if !expected_keys.is_empty() {
        panic!("{}", format!("Iterator only have {} elements. Expected number of elements: {}",
                             current_index + 1, initial_expected_n_elements))
    }
}