use std::collections::HashSet;
use bytes::Bytes;
use shared::SimpleDbError;
use crate::{Row, Schema};
use crate::sql::plan::plan_step::{PlanStep, PlanStepDesc, PlanStepTrait};

//Given two plan steps this step performs the set "union" operation by the primary key.
//Produces sorted rows if both source plans, produced rows sorted by the same key
#[derive(Clone)]
pub struct MergeUnionStep {
    pub(crate) plans: Vec<PlanStep>,
    pub(crate) should_produce_sorted_rows: bool,
    //Avoid duplicates, indexed by row primary_keys
    pub(crate) returned_primary_keys: HashSet<Bytes>,

    //Used when should_produce_sorted_rows set to true
    pub(crate) sort_column_name: String,
    pub(crate) pending_rows_to_return_right: Vec<Row>,
    pub(crate) pending_rows_to_return_left: Vec<Row>,
    pub(crate) last_returned: Option<Bytes>,

    //Used when should_produce_sorted_rows set to false
    pub(crate) prev_plan_index_returned: usize,
}

impl MergeUnionStep {
    pub(crate) fn create(
        schema: &Schema,
        a: PlanStep,
        b: PlanStep,
    ) -> Result<MergeUnionStep, SimpleDbError> {
        let mut plans = Vec::new();
        plans.push(a);
        plans.push(b);
        
        let is_sorted = PlanStep::get_column_sorted_by_plans(schema, &plans[0], &plans[1]);

        Ok(MergeUnionStep {
            sort_column_name: if is_sorted.is_some() { is_sorted.clone().unwrap() } else { String::from("") },
            should_produce_sorted_rows: is_sorted.is_some(),
            pending_rows_to_return_right: Vec::new(),
            pending_rows_to_return_left: Vec::new(),
            returned_primary_keys: HashSet::new(),
            prev_plan_index_returned: 0,
            last_returned: None,
            plans
        })
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

    fn next_sorted_row(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match (self.next_row_left_sorted()?, self.next_row_right_sorted()?) {
            (None, None) => Ok(None),
            (Some(left), None) => Ok(Some(left)),
            (None, Some(right)) => Ok(Some(right)),
            (Some(left), Some(right)) => {
                let sort_value_right = right.get_column_value(&self.sort_column_name)
                    .unwrap();
                let sort_value_left = left.get_column_value(&self.sort_column_name)
                    .unwrap();

                if sort_value_left.lt(&sort_value_right) {
                    self.pending_rows_to_return_right.push(right);
                    Ok(Some(left))
                } else {
                    self.pending_rows_to_return_left.push(left);
                    Ok(Some(right))
                }
            }
        }
    }

    fn next_row_left_sorted(&mut self) -> Result<Option<Row>, SimpleDbError>{
        if !self.pending_rows_to_return_left.is_empty() {
            Ok(Some(self.pending_rows_to_return_left.remove(0)))
        } else {
            self.plans[0].next()
        }
    }

    fn next_row_right_sorted(&mut self) -> Result<Option<Row>, SimpleDbError>{
        if !self.pending_rows_to_return_right.is_empty() {
            Ok(Some(self.pending_rows_to_return_right.remove(0)))
        } else {
            self.plans[1].next()
        }
    }

    fn next_not_sorted_row(&mut self) -> Result<Option<Row>, SimpleDbError> {
        while !self.plans.is_empty() {
            let current_plan_index = self.get_next_plan_index_to_return();

            match self.plans[current_plan_index].next()? {
                Some(row) => {
                    self.prev_plan_index_returned = current_plan_index;
                    return Ok(Some(row));
                },
                None => { self.plans.remove(current_plan_index); } //Remove last plan
            };
        }

        Ok(None)
    }
}

impl PlanStepTrait for MergeUnionStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        loop {
            let row = if self.should_produce_sorted_rows {
                self.next_sorted_row()
            } else {
                self.next_not_sorted_row()
            }?;

            match row {
                Some(row) => {
                    let row_primary_bytes = row.get_primary_column_value().get_bytes();
                    if !self.returned_primary_keys.contains(row_primary_bytes) {
                        self.returned_primary_keys.insert(row_primary_bytes.clone());
                        return Ok(Some(row))
                    } else {
                        //Else continue
                        //This can be removed because each row will be scanned at most twice
                        self.returned_primary_keys.remove(row_primary_bytes);
                    }
                }
                None => return Ok(None),
            }
        }
    }

    fn desc(&self) -> PlanStepDesc {
        let right = self.plans[1].desc();
        let left = self.plans[0].desc();
        PlanStepDesc::MergeUnion(Box::new(left), Box::new(right))
    }
}