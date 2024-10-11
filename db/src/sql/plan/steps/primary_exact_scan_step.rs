use crate::selection::Selection;
use crate::{Row};
use bytes::Bytes;
use shared::SimpleDbError;
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use crate::sql::plan::plan_step::{PlanStepDesc, PlanStepTrait};
use crate::table::table::Table;

pub struct PrimaryExactScanStep {
    row: Option<Row>,
    primary_key_value: Bytes,
}

impl PrimaryExactScanStep {
    pub(crate) fn create(
        table: Arc<Table>,
        id: Bytes,
        selection: Selection,
        transaction: &Transaction
    ) -> Result<PrimaryExactScanStep, SimpleDbError> {
        Ok(PrimaryExactScanStep {
            row: table.get_by_primary_column(&id, transaction, &selection)?,
            primary_key_value: id,
        })
    }
}

impl PlanStepTrait for PrimaryExactScanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        Ok(self.row.take())
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::PrimaryExactScan(self.primary_key_value.clone())
    }
}