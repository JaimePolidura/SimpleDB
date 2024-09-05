use std::sync::Arc;
use storage::lsm;
use storage::lsm::KeyspaceId;

pub struct Table {
    storage_keyspace_id: KeyspaceId,
    storage: Arc<lsm::Lsm>,
}