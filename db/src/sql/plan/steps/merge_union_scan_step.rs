use std::collections::HashSet;
use bytes::Bytes;
use shared::SimpleDbError;
use crate::Row;
use crate::sql::plan::plan_step::{Plan, PlanStep};

pub struct MergeUnionScanStep {
    plans: Vec<Plan>,
    returned_rows: HashSet<Bytes>,

    prev_plan_index_returned: usize,
}

impl MergeUnionScanStep {
    pub fn create(
        a: Plan,
        b: Plan,
    ) -> Result<Plan, SimpleDbError> {
        let mut plans = Vec::new();
        plans.push(a);
        plans.push(b);

        Ok(Box::new(MergeUnionScanStep {
            returned_rows: HashSet::new(),
            prev_plan_index_returned: 0,
            plans
        }))
    }

    fn get_next_plan_index_to_return(&self) -> usize {
        if self.plans.len() == 1 {
            return 0;
        }
        if self.prev_plan_index_returned == 0 {
            return 1;
        } else {
            return 0;
        }
    }
}

impl PlanStep for MergeUnionScanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        while !self.plans.is_empty() {
            let current_plan_index = self.get_next_plan_index_to_return();
            match &mut self.plans[current_plan_index].next()? {
                Some(row) => {
                    let row_primary_key = row.get_primary_column_value();

                    if !self.returned_rows.contains(row_primary_key) {
                        self.prev_plan_index_returned = current_plan_index;
                        return Ok(Some(row.clone()));
                    } else {
                        //This can be removed because each row will be scanned at most twice
                        self.returned_rows.remove(row_primary_key);
                    }
                },
                None => { self.plans.remove(current_plan_index); } //Remove last plan
            };
        }

        //Both plans are empty
        return Ok(None);
    }
}