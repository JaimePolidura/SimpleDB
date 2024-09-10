use crate::table::table_descriptor::{ColumnType, TableDescriptor};
use shared::SimpleDbError;
use std::sync::Arc;

pub struct Table {
    storage_keyspace_id: shared::KeyspaceId,
    table_descriptor: TableDescriptor,
    storage: Arc<storage::Storage>,
}

impl Table {
    pub fn add_column(
        &self,
        column_name: &str,
        column_type: ColumnType
    ) -> Result<(), SimpleDbError> {
        self.table_descriptor.add_column(self.storage_keyspace_id, column_name, column_type)
    }

    pub fn create(
        table_name: &str,
        options: &Arc<shared::SimpleDbOptions>,
        storage: &Arc<storage::Storage>,
    ) -> Result<Arc<Table>, SimpleDbError> {
        let table_keyspace_id = storage.create_keyspace()?;
        let table_descriptor = TableDescriptor::create(
            table_keyspace_id,
            options,
            table_name
        )?;

        Ok(Arc::new(Table{
            storage_keyspace_id: table_keyspace_id,
            storage: storage.clone(),
            table_descriptor,
        }))
    }

    pub fn load_tables (
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