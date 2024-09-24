use crate::table::record::Record;
use crate::table::table::Table;
use bytes::Bytes;
use shared::ColumnId;
use std::sync::Arc;
use crate::table::table_descriptor::ColumnDescriptor;

#[derive(Clone)]
pub struct Row {
    pub(crate) storage_engine_record: Record,
    pub(crate) key_bytes: Bytes,

    pub(crate) selection: Arc<Vec<ColumnId>>,
    pub(crate) table: Arc<Table>
}

impl Row {
    pub(crate) fn create(
        storage_engine_record: Record,
        selection: &Arc<Vec<ColumnId>>,
        table: &Arc<Table>,
        key_bytes: Bytes
    ) -> Row {
        Row {
            selection: selection.clone(),
            storage_engine_record,
            table: table.clone(),
            key_bytes
        }
    }

    pub fn get_primary_column_value(&self) -> &Bytes {
        &self.key_bytes
    }

    //Expect column_name to have been validated before calling this function
    pub fn get_column_value(&self, column_name: &str) -> Option<&Bytes> {
        let column_id = *self.table.columns_by_name.get(column_name).unwrap()
            .value();
        self.storage_engine_record.get_value(column_id)
    }

    pub fn get_column_desc(&self, column_name: &str) -> Option<ColumnDescriptor> {
        self.table.get_column_desc(column_name)
    }
}