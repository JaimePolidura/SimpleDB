pub fn u16_vec_to_u8_vec(u16_vec: &Vec<u16>) -> Vec<u8> {
    let mut u8_vec: Vec<u8> = Vec::with_capacity(u16_vec.len() * 2);

    for &val in u16_vec {
        u8_vec.extend_from_slice(&val.to_le_bytes());
    }

    u8_vec
}