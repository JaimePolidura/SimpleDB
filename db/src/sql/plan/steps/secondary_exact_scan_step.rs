use crate::index::secondary_index_iterator::SecondaryIndexIterator;
use crate::selection::Selection;
use crate::table::table::Table;
use crate::{Row, Type, Value};
use bytes::Bytes;
use shared::SimpleDbError;
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use storage::SimpleDbStorageIterator;
use crate::sql::plan::plan_step::{PlanStepDesc, PlanStepTrait};

pub struct SecondaryExactScanStep {
    secondary_index_iterator: SecondaryIndexIterator<SimpleDbStorageIterator>,
    transaction: Transaction,
    selection: Selection,
    table: Arc<Table>,
    secondary_column_name: String,
    secondary_index_value_lookup: Bytes,
}

impl SecondaryExactScanStep {
    pub(crate) fn create(
        table: Arc<Table>,
        secondary_column_name: &str,
        secondary_index_value_lookup: Bytes,
        transaction: &Transaction,
        selection: Selection
    ) -> Result<SecondaryExactScanStep, SimpleDbError> {
        let secondary_index_iterator = table.scan_from_key_secondary_index(
            &secondary_index_value_lookup,
            true,
            transaction,
            secondary_column_name
        )?;

        Ok(SecondaryExactScanStep {
            secondary_column_name: secondary_column_name.to_string(),
            transaction: transaction.clone(),
            secondary_index_iterator,
            secondary_index_value_lookup,
            selection,
            table,
        })
    }
}

impl PlanStepTrait for SecondaryExactScanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        while let Some((secondary_indexed_value, primary_key)) = self.secondary_index_iterator.next() {
            if !secondary_indexed_value.as_bytes().eq(&self.secondary_index_value_lookup) {
                return Ok(None);
            }

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

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::SecondaryExactExactScan(
            self.secondary_column_name.clone(),
            self.secondary_index_value_lookup.clone(),
        )
    }
}