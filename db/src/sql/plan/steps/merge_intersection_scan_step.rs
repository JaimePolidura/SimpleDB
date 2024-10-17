use std::collections::HashMap;
use bytes::Bytes;
use shared::SimpleDbError;
use crate::Row;
use crate::sql::plan::plan_step::{PlanStep, PlanStepDesc, PlanStepTrait};

#[derive(Clone)]
pub struct MergeIntersectionStep {
    pub(crate) plans: Vec<PlanStep>,
    pub(crate) rows_not_intersected: HashMap<Bytes, Row>,
    pub(crate) prev_plan_index: usize,
}

impl MergeIntersectionStep {
    pub(crate) fn create(
        a: PlanStep,
        b: PlanStep,
    ) -> Result<MergeIntersectionStep, SimpleDbError> {
        let mut plans = Vec::new();
        plans.push(a);
        plans.push(b);

        Ok(MergeIntersectionStep {
            rows_not_intersected: HashMap::new(),
            prev_plan_index: 0,
            plans,
        })
    }

    fn get_next_index(&self, prev_index: usize) -> usize {
        if prev_index == 0 {
            return 1;
        } else {
            return 0;
        }
    }
}

impl PlanStepTrait for MergeIntersectionStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        let mut current_index = self.prev_plan_index;

        //If there are less than 2 plans, no more rows will be intersected, so we can return None
        while self.plans.len() == 2 {
            current_index = self.get_next_index(current_index);
            let current_plan = &mut self.plans[current_index];

            match current_plan.next()? {
                Some(current_row) => {
                    let current_row_primary_value = current_row.get_primary_column_value();
                    if let Some(row_intersected) = self.rows_not_intersected.remove(current_row_primary_value.get_bytes()) {
                        self.prev_plan_index = current_index;
                        return Ok(Some(row_intersected)); //Found intersection
                    } else {
                        self.rows_not_intersected.insert(current_row_primary_value.get_bytes().clone(), current_row);
                    }
                }
                None => {
                    self.plans.remove(current_index);
                    break;
                },
            }
        }

        Ok(None)
    }

    fn desc(&self) -> PlanStepDesc {
        let right = self.plans[1].desc();
        let left = self.plans[0].desc();
        PlanStepDesc::MergeIntersection(Box::new(left), Box::new(right))
    }
}