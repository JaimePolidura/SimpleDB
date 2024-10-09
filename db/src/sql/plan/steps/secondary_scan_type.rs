use crate::index::secondary_index_iterator::SecondaryIndexIterator;
use crate::selection::Selection;
use crate::sql::plan::plan_step::{Plan, PlanStep};
use crate::table::table::Table;
use crate::Row;
use bytes::Bytes;
use shared::SimpleDbError;
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use storage::SimpleDbStorageIterator;

pub struct SecondaryExactScanType {
    secondary_index_iterator: SecondaryIndexIterator<SimpleDbStorageIterator>,
    transaction: Transaction,
    selection: Selection,
    table: Arc<Table>,
}

impl SecondaryExactScanType {
    pub fn create(
        table: Arc<Table>,
        secondary_column_name: &str,
        secondary_value: Bytes,
        transaction: &Transaction,
        selection: Selection
    ) -> Result<Plan, SimpleDbError> {
        let secondary_index_iterator = table.scan_from_key_secondary_index(
            &secondary_value,
            transaction,
            secondary_column_name
        )?;

        Ok(Box::new(SecondaryExactScanType {
            transaction: transaction.clone(),
            secondary_index_iterator,
            selection,
            table,
        }))
    }
}


impl PlanStep for SecondaryExactScanType {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        while let Some(primary_key) = self.secondary_index_iterator.next() {
            let mut primary_key_iterator = self.table.scan_from_key(
                &primary_key.as_bytes(),
                true,
                &self.transaction,
                &self.selection,
            )?;

            primary_key_iterator.next();

            return Ok(Some(primary_key_iterator.row().clone()));
        }

        Ok(None)
    }
}