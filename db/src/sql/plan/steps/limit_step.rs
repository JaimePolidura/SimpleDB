use shared::SimpleDbError;
use crate::Row;
use crate::sql::plan::plan_step::PlanStep;
use crate::sql::statement::Limit;

pub struct LimitStep {
    limit: Limit,
    source: Box<dyn PlanStep>,

    count: usize
}

impl LimitStep {
    pub fn create(
        limit: Limit,
        source: Box<dyn PlanStep>
    ) -> LimitStep {
        LimitStep {
            count: 0,
            source,
            limit,
        }
    }
}

impl PlanStep for LimitStep {
    fn next(&mut self) -> Result<Option<&Row>, SimpleDbError> {
        match self.limit {
            Limit::Some(limit) => {
                if limit > self.count {
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
}