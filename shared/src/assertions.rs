use crate::iterators::storage_iterator::StorageIterator;

pub fn assert_iterator_key_seq<I>(
    mut iterator: I,
    mut expected_keys: Vec<&str>
)
where
    I: StorageIterator
{
    let initial_expected_n_elements = expected_keys.len();
    let mut current_index = -1;

    while !expected_keys.is_empty() && iterator.next() {
        let actual = String::from_utf8(iterator.key().as_bytes().to_vec())
            .unwrap();
        let expected = expected_keys.remove(0);
        current_index += 1;

        if actual != expected {
            assert_eq!(actual, expected, "{}", format!("Expected key: {} Actual: {} Index: {}",
                                                       expected.to_string(), actual, current_index.to_string()));
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

pub fn assert_iterator_key_value_seq<I>(
    mut iterator: I,
    mut values: Vec<(&str, Vec<u8>)>
)
where
    I: StorageIterator
{
    let initial_expected_n_elements = values.len();
    let mut current_index = -1;

    while !values.is_empty() && iterator.next() {
        let actual_key = String::from_utf8(iterator.key().as_bytes().to_vec())
            .unwrap();
        let actual_value = iterator.value().to_vec();

        let (expected_key, expected_value) = values.remove(0);
        current_index += 1;

        if expected_key != actual_key {
            assert_eq!(actual_key, expected_key, "{}", format!("Expected key: {} Actual: {} Index: {}",
                                                             expected_key.to_string(), actual_key, current_index.to_string()));
        }
        if expected_value != actual_value {
            assert_eq!(actual_value, expected_value, "{}", format!("Expected value: {:?} Actual: {:?} Index: {}",
                                                                 expected_value, actual_value, current_index.to_string()));
        }

        if !values.is_empty() {
            assert!(iterator.has_next());
        }
    }

    if iterator.has_next() {
        panic!("Iterator still have values")
    }
    if !values.is_empty() {
        panic!("{}", format!("Iterator only have {} elements. Expected number of elements: {}",
                             current_index + 1, initial_expected_n_elements))
    }
}