use std::path::PathBuf;

pub fn get_directory_usize(
    base_path: &String,
    keyspace_id: usize,
) -> PathBuf {
    let mut path_buff = PathBuf::from(base_path.as_str());
    path_buff.push(keyspace_id.to_string());
    path_buff
}

pub fn get_file_usize(
    base_path: &String,
    keyspace_id: usize,
    name: &str
) -> PathBuf {
    let mut path_buff = PathBuf::from(base_path.as_str());
    path_buff.push(keyspace_id.to_string());
    path_buff.push(name);
    path_buff
}