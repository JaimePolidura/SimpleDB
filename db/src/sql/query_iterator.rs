use shared::SimpleDbError;
use crate::{Row};
use crate::sql::plan::plan_step::{PlanStep, PlanStepDesc};
use crate::table::schema::Schema;

//This will be returned to the user of SimpleDb when it queries data
//This is simple wrapper around a Plan
pub struct QueryIterator {
    plan: PlanStep,
    schema: Schema
}

impl QueryIterator {
    pub fn create(plan: PlanStep, columns_descriptor_selection: Schema) -> QueryIterator {
        QueryIterator { plan, schema: columns_descriptor_selection }
    }

    pub fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        self.plan.next()
    }

    pub fn next_n(&mut self, n: usize) -> Result<Vec<Row>, SimpleDbError> {
        let mut results = Vec::new();

        while results.len() <= n {
            match self.plan.next()? {
                Some(row) => results.push(row),
                None => break
            };
        }

        Ok(results)
    }

    pub fn all(&mut self) -> Result<Vec<Row>, SimpleDbError> {
        let mut results = Vec::new();

        while let Some(row) = self.plan.next()? {
            results.push(row);
        }

        Ok(results)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_plan_desc(&self) -> PlanStepDesc {
        self.plan.desc()
    }
}