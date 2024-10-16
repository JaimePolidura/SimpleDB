use shared::SimpleDbError;
use crate::sql::plan::plan_step::PlanStep;

pub struct Optimizer {

}

impl Optimizer {
    pub fn create() -> Optimizer {
        Optimizer {}
    }

    pub fn optimize(&self, plan: PlanStep) -> Result<PlanStep, SimpleDbError> {

    }
}