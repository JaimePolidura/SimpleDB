use std::sync::Arc;
use shared::KeyspaceId;

pub struct SecondaryIndex {
    keyspace_id: KeyspaceId,
    storage: Arc<storage::Storage>
}

impl SecondaryIndex {
    pub fn create(
        keyspace_id: KeyspaceId,
        storage: Arc<storage::Storage>
    ) -> SecondaryIndex {
        SecondaryIndex { keyspace_id, storage }
    }
}