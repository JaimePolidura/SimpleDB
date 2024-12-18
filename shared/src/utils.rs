use bytes::Bytes;
use std::fs;
use std::fs::DirEntry;
use std::path::PathBuf;
use crossbeam_skiplist::{SkipMap, SkipSet};

pub fn bytes_to_u8_array_string(bytes: &Bytes) -> String {
    let mut array = Vec::new();
    for value in bytes.to_vec() {
        array.push(value.to_string());
    }

    format!("[{}]", array.join(", "))
}

pub fn bytes_to_f64_le(bytes: &Bytes) -> f64 {
    let byte_array: [u8; 8] = bytes[..8].try_into().expect("Slice must be 8 bytes long");
    f64::from_le_bytes(byte_array)
}

pub fn bytes_to_u64_le(bytes: &Bytes) -> u64 {
    u64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]])
}

pub fn bytes_to_i64_le(bytes: &Bytes) -> i64 {
    i64::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7]])
}

pub fn bytes_to_f32_le(bytes: &Bytes) -> f32 {
    f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

pub fn bytes_to_u32_le(bytes: &Bytes) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

pub fn bytes_to_i32_le(bytes: &Bytes) -> i32 {
    i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

pub fn bytes_to_u16_le(bytes: &Bytes) -> u16 {
    u16::from_le_bytes([bytes[0], bytes[1]])
}

pub fn bytes_to_i16_le(bytes: &Bytes) -> i16 {
    i16::from_le_bytes([bytes[0], bytes[1]])
}

pub fn bytes_to_u8(bytes: &Bytes) -> u8 {
    bytes[0]
}

pub fn bytes_to_i8(bytes: &Bytes) -> i8 {
    bytes[0] as i8
}

pub fn u16_vec_to_u8_vec(u16_vec: &Vec<u16>) -> Vec<u8> {
    let mut u8_vec: Vec<u8> = Vec::with_capacity(u16_vec.len() * 2);

    for &val in u16_vec {
        u8_vec.extend_from_slice(&val.to_le_bytes());
    }

    u8_vec
}

pub fn u16_to_u8_le(value: u16, start_index: usize, vector: &mut Vec<u8>) {
    fill_u8_vec_if_empty(vector, start_index + 1, 0);

    vector[start_index] = (value & 0xff) as u8;
    vector[start_index + 1] = (value >> 8 & 0xff) as u8;
}

pub fn overflows_bytes_64(bytes: &Bytes, target_size_bytes: u8) -> bool {
    let byte_array: [u8; 8] = bytes[..8].try_into().expect("slice with incorrect length");
    let u64 = u64::from_le_bytes(byte_array);
    let (rest, _)  = u64.overflowing_shr((target_size_bytes * 8) as u32);
    rest == 0x00
}

pub fn u64_to_u8_le(value: u64, start_index: usize, vector: &mut Vec<u8>) {
    fill_u8_vec_if_empty(vector, start_index + 7, 0);

    vector[start_index] =     (value & 0xff) as u8;
    vector[start_index + 1] = (value >> 8 & 0xff) as u8;
    vector[start_index + 2] = (value >> 16 & 0xff) as u8;
    vector[start_index + 3] = (value >> 24 & 0xff) as u8;
    vector[start_index + 4] = (value >> 32 & 0xff) as u8;
    vector[start_index + 5] = (value >> 40 & 0xff) as u8;
    vector[start_index + 6] = (value >> 48 & 0xff) as u8;
    vector[start_index + 7] = (value >> 56 & 0xff) as u8;
}

pub fn u8_vec_to_u16_le(vec: &Vec<u8>, start_index: usize) -> u16 {
    vec[start_index] as u16 | ((vec[start_index + 1] as u16) << 8)
}

pub fn u8_vec_to_u32_le(vec: &Vec<u8>, start_index: usize) -> u32 {
    vec[start_index] as u32 |
        ((vec[start_index + 1] as u32) << 8) |
        ((vec[start_index + 2] as u32) << 16) |
        ((vec[start_index + 3] as u32) << 24)
}

pub fn u8_vec_to_u64_le(vec: &Vec<u8>, start_index: usize) -> u64 {
    vec[start_index] as u64 |
        ((vec[start_index + 1] as u64) <<  8) |
        ((vec[start_index + 2] as u64) << 16) |
        ((vec[start_index + 3] as u64) << 24) |
        ((vec[start_index + 4] as u64) << 32) |
        ((vec[start_index + 5] as u64) << 40) |
        ((vec[start_index + 6] as u64) << 48) |
        ((vec[start_index + 7] as u64) << 56)
}

pub fn u8_vec_to_u16_vec(u8_vec: &Vec<u8>) -> Vec<u16> {
    if u8_vec.len() % 2 != 0 {
        panic!("Vector's length in method u8_vec_to_u16_vec() expeted to be even");
    }

    let mut values: Vec<u16> = Vec::new();

    for current_start_index in 0..(u8_vec.len() / 2) {
        values.push(u8_vec_to_u16_le(u8_vec, current_start_index * 2));
    }

    values
}

pub fn extract_number_from_file_name(
    file: &DirEntry,
    separator: &str
) -> Result<usize, ()> {
    let file_name = file.file_name();
    let split = file_name
        .to_str()
        .unwrap()
        .split(separator)
        .last();

    if split.is_some() {
        split.unwrap()
            .parse::<usize>()
            .map_err(|_| ())
    } else {
        Err(())
    }
}

pub fn pop_front<T>(vec: &mut Vec<T>) -> Option<T> {
    if vec.is_empty() {
        None
    } else {
        Some(vec.remove(0))
    }
}

pub fn hash(key: &[u8]) -> u32 {
    farmhash::hash32(key)
}

pub fn fill_vec<T>(vec: &mut Vec<T>, size: usize, value: T)
where
    T: Copy
{
    for _ in 0..size {
        vec.push(value);
    }
}

pub fn fill_u8_vec_if_empty<T>(vec: &mut Vec<T>, index: usize, value: T)
where
    T: Copy
{
    if index >= vec.len() {
        for _ in 0..index - vec.len() + 1 {
            vec.push(value);
        }
    }
}

pub fn enum_eq<T>(a: &T, b: &T) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

pub fn create_paths(path: &String) -> Result<(), std::io::Error> {
    let path = PathBuf::from(path);
    let path = path.as_path();
    fs::create_dir_all(path)
}

pub fn clone_skipset<V>(source: &SkipSet<V>) -> SkipSet<V>
where
    V: Clone + Ord + Send + 'static
{
    let mut result: SkipSet<V> = SkipSet::new();

    for entry in source.iter() {
        result.insert(entry.value().clone());
    }

    result
}

pub fn clone_skipmap<K, V>(source: &SkipMap<K, V>) -> SkipMap<K, V>
where
    K: Clone + Ord + Send + 'static,
    V: Clone + Send + 'static
{
    let mut result: SkipMap<K, V> = SkipMap::new();

    for entry in source.iter() {
        let key = entry.key();
        let value = entry.value();

        result.insert(key.clone(), value.clone());
    }

    result
}