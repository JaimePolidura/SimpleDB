use std::path::PathBuf;
use std::sync::Arc;
use crate::keyspace::keyspace::KeyspaceId;
use crate::lsm_options::LsmOptions;

pub fn get_path(
    lsm_options: &Arc<LsmOptions>,
    keyspace_id: KeyspaceId,
    name: &str
) -> PathBuf {
    let mut base_path = PathBuf::from(lsm_options.base_path.as_str());
    base_path.push(keyspace_id.to_string());
    base_path.push(name);
    base_path
}