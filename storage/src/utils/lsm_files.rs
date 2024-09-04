use std::path::PathBuf;
use std::sync::Arc;
use crate::lsm::KeyspaceId;
use crate::lsm_options::LsmOptions;

pub fn get_keyspace_directory(
    lsm_options: &Arc<LsmOptions>,
    keyspace_id: KeyspaceId,
) -> PathBuf {
    let mut path_buff = PathBuf::from(lsm_options.base_path.as_str());
    path_buff.push(keyspace_id.to_string());
    path_buff
}

pub fn get_keyspace_file(
    lsm_options: &Arc<LsmOptions>,
    keyspace_id: KeyspaceId,
    name: &str
) -> PathBuf {
    let mut path_buff = PathBuf::from(lsm_options.base_path.as_str());
    path_buff.push(keyspace_id.to_string());
    path_buff.push(name);
    path_buff
}