use crate::table::table_descriptor::TableDescriptor;
use shared::SimpleDbError;
use std::sync::Arc;

pub struct Table {
    storage_keyspace_id: shared::KeyspaceId,
    storage: Arc<storage::Storage>,
    table_descriptor: TableDescriptor,
}

impl Table {
    pub fn load_tables(
        options: &Arc<shared::SimpleDbOptions>,
        storage: &Arc<storage::Storage>,
    ) -> Result<Vec<Arc<Table>>, SimpleDbError> {
        let mut tables = Vec::new();

        for keysapce_id in storage.get_keyspaces_id() {
            tables.push(Arc::new(Table {
                table_descriptor: TableDescriptor::load_table_descriptor(options, keysapce_id)?,
                storage_keyspace_id: keysapce_id,
                storage: storage.clone(),
            }));
        }

        Ok(tables)
    }

    pub fn name(&self) -> String {
        self.table_descriptor.name()
    }
}