use crate::index::secondary_index_iterator::SecondaryIndexIterator;
use crate::selection::Selection;
use crate::sql::plan::plan_step::{PlanStepDesc, PlanStepTrait};
use crate::sql::plan::scan_type::{RangeKeyPosition, RangeScan};
use crate::table::table::Table;
use crate::Row;
use shared::{SimpleDbError, Type, Value};
use std::sync::Arc;
use shared::key::Key;
use storage::transactions::transaction::Transaction;
use storage::{SimpleDbStorageIterator, Storage};

pub struct SecondaryRangeScanStep {
    secondary_iterator: SecondaryIndexIterator<SimpleDbStorageIterator>,
    selection: Selection,
    range: RangeScan,
    table: Arc<Table>,
    transaction: Transaction
}

impl SecondaryRangeScanStep {
    pub(crate) fn create(
        table: Arc<Table>,
        selection: Selection,
        column_name: &str,
        transaction: &Transaction,
        range: RangeScan,
    ) -> Result<SecondaryRangeScanStep, SimpleDbError> {
        let iterator = if let Some(star_range_key_expr) = range.start() {
            let star_range_key_bytes = star_range_key_expr.get_literal_bytes();
            table.scan_from_key_secondary_index(&star_range_key_bytes, range.is_start_inclusive(), transaction, &column_name)
        } else {
            table.scan_all_secondary_index(transaction, &column_name)
        }?;

        Ok(SecondaryRangeScanStep {
            transaction: transaction.clone(),
            secondary_iterator: iterator,
            selection,
            table,
            range,
        })
    }

    fn get_row_by_primary(&self, primary_key: Key) -> Result<Option<Row>, SimpleDbError> {
        self.table.get_by_primary_column(
            primary_key.as_bytes(),
            &self.transaction,
            &self.selection,
        )
    }
}

impl PlanStepTrait for SecondaryRangeScanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        if let Some((indexed_value, primary_key)) = self.secondary_iterator.next() {
            match self.range.get_position(indexed_value.get_value()) {
                RangeKeyPosition::Inside => self.get_row_by_primary(primary_key),
                RangeKeyPosition::Above => Ok(None),
                //Not possible because, the iterator have been seeked in construction time
                RangeKeyPosition::Bellow => panic!(""),
            }
        } else {
            Ok(None)
        }

    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::RangeScan(self.range.clone())
    }
}