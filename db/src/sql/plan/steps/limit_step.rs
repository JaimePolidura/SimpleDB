use shared::SimpleDbError;
use crate::Row;
use crate::sql::plan::plan_step::{PlanStep, PlanStepDesc, PlanStepTrait};
use crate::sql::parser::statement::Limit;

pub struct LimitStep {
    limit: Limit,
    source: PlanStep,

    count: usize
}

impl LimitStep {
    pub(crate) fn create(
        limit: Limit,
        source: PlanStep
    ) -> LimitStep {
        LimitStep {
            count: 0,
            source,
            limit,
        }
    }
}

impl PlanStepTrait for LimitStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match self.limit {
            Limit::Some(limit) => {
                if (self.count + 1) > limit {
                    Ok(None)
                } else if let Some(next_row) = self.source.next()? {
                    self.count += 1;
                    Ok(Some(next_row))
                } else {
                    Ok(None)
                }
            },
            Limit::None => self.source.next(),
        }
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::Limit(
            self.limit.clone(),
            Box::new(self.source.desc())
        )
    }
}