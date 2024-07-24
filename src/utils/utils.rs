pub fn u16_vec_to_u8_vec(u16_vec: &Vec<u16>) -> Vec<u8> {
    let mut u8_vec: Vec<u8> = Vec::with_capacity(u16_vec.len() * 2);

    for &val in u16_vec {
        u8_vec.extend_from_slice(&val.to_le_bytes());
    }

    u8_vec
}

pub fn u16_to_u8_le(value: u16, start_index: usize, vector: &mut Vec<u8>) {
    fill_u8_vec_if_emtpy(vector, start_index + 1, 0);

    vector[start_index] = (value & 0xff) as u8;
    vector[start_index + 1] = (value >> 8 & 0xff) as u8;
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

pub fn fill_u8_vec_if_emtpy<T>(vec: &mut Vec<T>, index: usize, value: T)
where
    T: Copy
{
    if index >= vec.len() {
        for i in 0..index - vec.len() + 1 {
            vec.push(value);
        }
    }
}