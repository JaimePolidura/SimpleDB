use crate::selection::Selection;
use crate::table::table::Table;
use crate::table::table_iterator::TableIterator;
use crate::Row;
use shared::SimpleDbError;
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use storage::SimpleDbStorageIterator;
use crate::sql::plan::plan_step::{PlanStepDesc, PlanStepTrait};

#[derive(Clone)]
pub struct FullScanStep {
    pub(crate) iterator: TableIterator<SimpleDbStorageIterator>,
}

impl FullScanStep {
    pub(crate) fn create(
        table: Arc<Table>,
        selection: Selection,
        transaction: &Transaction
    ) -> Result<FullScanStep, SimpleDbError> {
        Ok(FullScanStep {
            iterator: table.scan_all(transaction, &selection)?
        })
    }
}

impl PlanStepTrait for FullScanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        if self.iterator.next() {
            Ok(Some(self.iterator.row().clone()))
        } else {
            Ok(None)
        }
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::FullScan
    }
}