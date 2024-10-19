use std::collections::HashMap;
use bytes::Bytes;
use shared::{SimpleDbError, Value};
use crate::{Row, Schema};
use crate::sql::plan::plan_step::{PlanStep, PlanStepDesc, PlanStepTrait};

//Given two plan steps this step performs the set "intersection" operation by the primary key.
//Produces sorted rows if both source plans, produced rows sorted by the same key
#[derive(Clone)]
pub struct MergeIntersectionStep {
    pub(crate) plans: Vec<PlanStep>,
    pub(crate) should_produce_sorted_rows: bool,

    //Used when should_produce_sorted_rows set to true
    pub(crate) sort_column_name: String,
    pub(crate) smaller: Option<Box<PlanStep>>,
    pub(crate) greater: Option<Box<PlanStep>>,
    pub(crate) last_greater_value: Option<Value>,

    //Used when should_produce_sorted_rows set to false
    pub(crate) rows_not_intersected: HashMap<Bytes, Row>,
    pub(crate) prev_plan_index: usize,
}

impl MergeIntersectionStep {
    pub(crate) fn create(
        schema: &Schema,
        a: PlanStep,
        b: PlanStep,
    ) -> Result<MergeIntersectionStep, SimpleDbError> {
        let mut plans = Vec::new();
        plans.push(a);
        plans.push(b);

        let is_sorted = PlanStep::get_column_sorted_by_plans(schema, &plans[0], &plans[1]);

        Ok(MergeIntersectionStep {
            should_produce_sorted_rows: is_sorted.is_some(),
            sort_column_name: if is_sorted.is_some() { is_sorted.unwrap() } else { String::from("") },
            rows_not_intersected: HashMap::new(),
            last_greater_value: None,
            prev_plan_index: 0,
            smaller: None,
            greater: None,
            plans,
        })
    }

    fn next_sorted_row(&mut self) -> Result<Option<Row>, SimpleDbError> {
        //Init "merge join" data
        if self.smaller.is_none() && self.greater.is_none() {
            let mut smaller = self.plans.remove(0);
            let greater = self.plans.remove(0);
            let smaller_row = smaller.next()?;
            let greater_row = smaller.next()?;

            //There won't be any intersection
            if smaller_row.is_none() || greater_row.is_none() {
                return Ok(None);
            }

            let row_from_greater = greater_row.unwrap();
            let value_from_greater = row_from_greater
                .get_column_value(&self.sort_column_name)
                .unwrap();
            let row_from_smaller = smaller_row.unwrap();
            let value_from_smaller = row_from_smaller
                .get_column_value(&self.sort_column_name)
                .unwrap();

            if value_from_greater.eq(&value_from_smaller) {
                self.last_greater_value = Some(value_from_greater);
                self.greater = Some(Box::new(greater));
                self.smaller = Some(Box::new(smaller));
                return Ok(Some(row_from_smaller));
            }

            if value_from_greater.ge(&value_from_smaller) {
                self.last_greater_value = Some(value_from_greater);
                self.greater = Some(Box::new(greater));
                self.smaller = Some(Box::new(smaller));
            } else {
                self.last_greater_value = Some(value_from_smaller);
                self.greater = Some(Box::new(smaller));
                self.smaller = Some(Box::new(greater));
            }
        }

        let mut current_value_greater = self.last_greater_value.take().unwrap();
        let mut smaller = self.smaller.take().unwrap();
        let mut greater = self.greater.take().unwrap();

        loop {
            let mut current_row_smaller = smaller.next()?;

            match current_row_smaller {
                Some(current_row_smaller) => {
                    let current_value_smaller = current_row_smaller.get_column_value(&self.sort_column_name)
                        .unwrap();

                    if current_value_smaller.eq(&current_value_greater) {
                        self.last_greater_value = Some(current_value_smaller);
                        self.smaller = Some(smaller);
                        self.greater = Some(greater);
                        return Ok(Some(current_row_smaller));
                    }
                    if current_value_smaller.gt(&current_value_greater) {
                        //Swap greater and smaller
                        current_value_greater = current_value_smaller;
                        let temp = smaller;
                        smaller = greater;
                        greater = temp;
                    }
                },
                None => { return Ok(None); },
            };
        }
    }

    fn next_not_sorted_row(&mut self) -> Result<Option<Row>, SimpleDbError> {
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
        if self.should_produce_sorted_rows {
            self.next_sorted_row()
        } else {
            self.next_not_sorted_row()
        }
    }

    fn desc(&self) -> PlanStepDesc {
        let right = self.plans[1].desc();
        let left = self.plans[0].desc();
        PlanStepDesc::MergeIntersection(Box::new(left), Box::new(right))
    }
}