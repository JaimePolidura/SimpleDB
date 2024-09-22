use shared::SimpleDbError;
use crate::Row;
use crate::sql::plan::plan_step::PlanStep;

pub struct RangeScan {

}

impl RangeScan {
    pub fn create(
    ) -> Result<RangeScan, SimpleDbError> {
        todo!()
    }
}

impl PlanStep for RangeScan {
    fn next(&mut self) -> Option<Row> {
        todo!()
    }
}