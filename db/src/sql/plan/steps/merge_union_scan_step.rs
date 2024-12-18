use std::collections::HashSet;
use bytes::Bytes;
use shared::SimpleDbError;
use crate::{Row, Schema};
use crate::sql::plan::plan_step::{PlanStep, PlanStepDesc, PlanStepTrait};
use crate::table::row::RowIterator;

//Given two plan steps this step performs the set "union" operation by the primary key.
//Produces sorted rows if the source plans produce rows sorted by the same key.
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

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use shared::Value;
    use crate::{Column, Row, Schema};
    use crate::sql::plan::plan_step::{MockStep, PlanStep, PlanStepTrait};
    use crate::sql::plan::steps::merge_union_scan_step::MergeUnionStep;
    use crate::table::record::Record;

    //left:  1 3 5 6
    //right: 2 3 4 7 9
    //Expected: 1 2 3 4 5 6 7 9
    #[test]
    fn should_sort() {
        let schema = Schema::create(vec![
            Column::create_primary("ID")
        ]);

        let left = PlanStep::Mock(MockStep::create(true, vec![
            row(&schema, 1), row(&schema, 3), row(&schema, 5), row(&schema, 6)
        ]));
        let right = PlanStep::Mock(MockStep::create(true, vec![
            row(&schema, 2), row(&schema, 3), row(&schema, 4), row(&schema, 7), row(&schema, 9)
        ]));

        let mut union_step = MergeUnionStep::create(&schema, left, right)
            .unwrap();

        assert_row_id(&mut union_step, 1);
        assert_row_id(&mut union_step, 2);
        assert_row_id(&mut union_step, 3);
        assert_row_id(&mut union_step, 4);
        assert_row_id(&mut union_step, 5);
        assert_row_id(&mut union_step, 6);
        assert_row_id(&mut union_step, 7);
        assert_row_id(&mut union_step, 9);
        assert_emtpy(&mut union_step);
    }

    fn assert_emtpy(step: &mut MergeUnionStep) {
        let row = step.next().unwrap();
        assert!(row.is_none());
    }

    fn assert_row_id(step: &mut MergeUnionStep, expected_id: i64) {
        let row = step.next().unwrap();
        let row = row.unwrap();
        assert_eq!(row.get_primary_column_value().get_i64().unwrap(), expected_id);
    }

    fn row(schema: &Schema, id: i64) -> Row {
        let mut record_builder = Record::builder();
        record_builder.add_column(0, Bytes::from(id.to_le_bytes().to_vec()));
        Row::create(record_builder.build(), Value::create_i64(id), schema.clone())
    }
}