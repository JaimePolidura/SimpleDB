use std::sync::Arc;
use shared::{ColumnId, KeyspaceId};
use crate::table::table::Table;

pub struct IndexCreationTask {
    table: Arc<Table>,
    column_id: ColumnId,
    keyspace_id: KeyspaceId,
}

impl IndexCreationTask {
    pub fn create(
        table: Arc<Table>,
        column_id: ColumnId,
        keyspace_id: KeyspaceId,
    ) -> IndexCreationTask {
        IndexCreationTask { table, column_id, keyspace_id }
    }
    
    pub fn start(&self) {
    }
}