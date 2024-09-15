use std::sync::Arc;
use shared::ColumnId;
use crate::table::record::Record;
use crate::table::table::Table;

pub struct Row {
    storage_engine_record: Record,
    selection: Arc<Vec<ColumnId>>,
    table: Arc<Table>
}

impl Row {
    pub fn create(
        storage_engine_record: Record,
        selection: &Arc<Vec<ColumnId>>,
        table: &Arc<Table>
    ) -> Row {
        Row {
            selection: selection.clone(),
            storage_engine_record,
            table: table.clone(),
        }
    }
}