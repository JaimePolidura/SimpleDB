use shared::SimpleDbError;
use crate::Row;
use crate::sql::plan::plan_step::{Plan, PlanStep};
use crate::sql::statement::Limit;

pub struct LimitStep {
    limit: Limit,
    source: Plan,

    count: usize
}

impl LimitStep {
    pub fn create(
        limit: Limit,
        source: Plan
    ) -> Plan {
        Box::new(LimitStep {
            count: 0,
            source,
            limit,
        })
    }
}

impl PlanStep for LimitStep {
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
}