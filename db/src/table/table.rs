use std::sync::Arc;

pub struct Table {
    storage_keyspace_id: storage::KeyspaceId,
    storage: Arc<storage::Storage>,
}