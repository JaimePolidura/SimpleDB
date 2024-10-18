use crate::index::secondary_index_iterator::SecondaryIndexIterator;
use crate::selection::{IndexSelectionType, Selection};
use crate::table::table::Table;
use bytes::Bytes;
use shared::{SimpleDbError, Value};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use storage::SimpleDbStorageIterator;
use crate::Row;
use crate::sql::plan::plan_step::{PlanStepDesc, PlanStepTrait};
use crate::table::row::RowBuilder;

#[derive(Clone)]
pub struct SecondaryExactScanStep {
    pub(crate) secondary_index_iterator: SecondaryIndexIterator<SimpleDbStorageIterator>,
    pub(crate) transaction: Transaction,
    pub(crate) selection: Selection,
    pub(crate) table: Arc<Table>,
    pub(crate) column_name: String,
    pub(crate) secondary_column_value: Value,
    pub(crate) index_selection_type: IndexSelectionType,
}

impl SecondaryExactScanStep {
    pub(crate) fn create(
        table: Arc<Table>,
        secondary_column_name: &str,
        secondary_index_value_lookup: Bytes,
        transaction: &Transaction,
        selection: Selection
    ) -> Result<SecondaryExactScanStep, SimpleDbError> {
        let column_type = table.get_schema().get_column_or_err(secondary_column_name)?
            .column_type;

        let secondary_index_iterator = table.scan_from_key_secondary_index(
            &secondary_index_value_lookup,
            true,
            transaction,
            secondary_column_name
        )?;
        let secondary_column_value = Value::create(secondary_index_value_lookup, column_type)?;

        Ok(SecondaryExactScanStep {
            index_selection_type: selection.get_index_selection_type(table.get_schema()),
            column_name: secondary_column_name.to_string(),
            transaction: transaction.clone(),
            secondary_index_iterator,
            secondary_column_value,
            selection,
            table,
        })
    }
}

impl PlanStepTrait for SecondaryExactScanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        while let Some((secondary_indexed_value, primary_key)) = self.secondary_index_iterator.next() {
            if !secondary_indexed_value.as_bytes().eq(self.secondary_column_value.get_bytes()) {
                return Ok(None);
            }

            match self.index_selection_type {
                IndexSelectionType::Primary |
                IndexSelectionType::Secondary |
                IndexSelectionType::OnlyPrimaryAndSecondary => {
                    let mut row_builder = RowBuilder::create(self.table.get_schema().clone());
                    row_builder.add_primary_value(primary_key.get_value().clone());
                    row_builder.add_by_column_name(secondary_indexed_value.get_value().get_bytes().clone(), &self.column_name);
                    return Ok(Some(row_builder.build()));
                }
                IndexSelectionType::All => {
                    let mut primary_key_iterator = self.table.scan_from_key(
                        &primary_key.as_bytes(),
                        true,
                        &self.transaction,
                        &self.selection,
                    )?;

                    primary_key_iterator.next();

                    return Ok(Some(primary_key_iterator.row().clone()));
                }
            }
        }

        Ok(None)
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::SecondaryExactExactScan(
            self.column_name.clone(),
            self.secondary_column_value.get_bytes().clone(),
        )
    }
}