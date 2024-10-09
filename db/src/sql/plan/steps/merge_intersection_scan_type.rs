use std::collections::HashMap;
use bytes::Bytes;
use shared::SimpleDbError;
use crate::Row;
use crate::sql::plan::plan_step::{Plan, PlanStep};

pub struct MergeIntersectionScanType {
    plans: Vec<Plan>,
    rows_not_intersected: HashMap<Bytes, Row>,
    prev_plan_index: usize,
}

impl MergeIntersectionScanType {
    pub fn create(
        a: Plan,
        b: Plan,
    ) -> Result<Plan, SimpleDbError> {
        let mut plans = Vec::new();
        plans.push(a);
        plans.push(b);

        Ok(Box::new(MergeIntersectionScanType {
            rows_not_intersected: HashMap::new(),
            prev_plan_index: 0,
            plans,
        }))
    }

    fn get_next_index(&self, prev_index: usize) -> usize {
        if prev_index == 0 {
            return 1;
        } else {
            return 0;
        }
    }
}

impl PlanStep for MergeIntersectionScanType {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        let mut current_index = self.prev_plan_index;

        //If there are less than 2 plans, no more rows will be intersected, so we can return None
        while self.plans.len() == 2 {
            current_index = self.get_next_index(current_index);
            let current_plan = &mut self.plans[current_index];

            match current_plan.next()? {
                Some(current_row) => {
                    let current_row_primary_value = current_row.get_primary_column_value();
                    if let Some(row_intersected) = self.rows_not_intersected.remove(current_row_primary_value) {
                        self.prev_plan_index = current_index;
                        return Ok(Some(row_intersected)); //Found intersection
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
}