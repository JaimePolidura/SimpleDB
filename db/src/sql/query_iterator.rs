use shared::SimpleDbError;
use crate::{ColumnDescriptor, Row};
use crate::sql::plan::plan_step::Plan;

//This will be returned to the user of SimpleDb when it queryes data
//This is simple wrapper around a Plan
pub struct QueryIterator {
    plan: Plan,
    columns_descriptor_selection: Vec<ColumnDescriptor>
}

impl QueryIterator {
    pub fn create(plan: Plan, columns_descriptor_selection: Vec<ColumnDescriptor>) -> QueryIterator {
        QueryIterator { plan, columns_descriptor_selection }
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

    pub fn columns_descriptor_selection(&self) -> &Vec<ColumnDescriptor> {
        &self.columns_descriptor_selection
    }
}