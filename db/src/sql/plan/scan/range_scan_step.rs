use std::sync::Arc;
use shared::SimpleDbError;
use storage::transactions::transaction::Transaction;
use crate::{Row, Table, TableIterator};
use crate::selection::Selection;
use crate::sql::plan::plan_step::PlanStep;
use crate::sql::plan::scan_type::{RangeKeyPosition, RangeScan};

pub struct RangeScanStep {
    range: RangeScan,
    iterator: TableIterator
}

impl RangeScanStep {
    pub fn create(
        table: Arc<Table>,
        selection: Selection,
        transaction: &Transaction,
        range: RangeScan
    ) -> Result<RangeScanStep, SimpleDbError> {
        let iterator = if let Some(star_range_key_expr) = range.start() {
            let star_range_key_bytes = star_range_key_expr.get_bytes();
            table.scan_from_key(&star_range_key_bytes, range.is_start_inclusive(), transaction, selection)
        } else {
            table.scan_all(transaction, selection)
        }?;

        Ok(RangeScanStep{
            iterator,
            range,
        })
    }
}

impl PlanStep for RangeScanStep {
    fn next(&mut self) -> Option<&Row> {
        if self.iterator.next() {
            let current_row = self.iterator.row();
            let current_primary_column_value = current_row.get_primary_column_value();

            match self.range.get_position(current_primary_column_value) {
                RangeKeyPosition::Inside => Some(current_row),
                RangeKeyPosition::Bellow => None,
                RangeKeyPosition::Above => None
            }
        } else {
            None
        }
    }
}