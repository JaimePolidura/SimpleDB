use std::sync::Arc;
use shared::SimpleDbError;
use storage::SimpleDbStorageIterator;
use storage::transactions::transaction::Transaction;
use crate::{Row};
use crate::selection::Selection;
use crate::sql::plan::plan_step::{Plan, PlanStep};
use crate::sql::plan::scan_type::{RangeKeyPosition, RangeScan};
use crate::table::table::Table;
use crate::table::table_iterator::TableIterator;

pub struct RangeScanStep {
    range: RangeScan,
    iterator: TableIterator<SimpleDbStorageIterator>
}

impl RangeScanStep {
    pub fn create(
        table: Arc<Table>,
        selection: Selection,
        transaction: &Transaction,
        range: RangeScan
    ) -> Result<Plan, SimpleDbError> {
        let iterator = if let Some(star_range_key_expr) = range.start() {
            let star_range_key_bytes = star_range_key_expr.serialize();
            table.scan_from_key(&star_range_key_bytes, range.is_start_inclusive(), transaction, &selection)
        } else {
            table.scan_all(transaction, selection)
        }?;

        Ok(Box::new(RangeScanStep{
            iterator,
            range,
        }))
    }
}

impl PlanStep for RangeScanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        if self.iterator.next() {
            let current_row = self.iterator.row();
            let current_primary_column_value = current_row.get_primary_column_value();

            match self.range.get_position(current_primary_column_value) {
                RangeKeyPosition::Inside => Ok(Some(current_row.clone())),
                RangeKeyPosition::Above => Ok(None),
                //Not possible because, the iterator have been seeked in construction time
                RangeKeyPosition::Bellow => panic!(""),
            }
        } else {
            Ok(None)
        }
    }
}