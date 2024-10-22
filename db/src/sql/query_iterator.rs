use crate::table::selection::Selection;
use crate::sql::plan::plan_step::{PlanStep, PlanStepDesc};
use crate::table::schema::Schema;
use crate::{Column, Row};
use shared::SimpleDbError;
use crate::table::row::RowIterator;

//This will be returned to the user of SimpleDb when it queries data
//This is simple wrapper around a Plan
#[derive(Clone)]
pub struct QueryIterator {
    source: PlanStep,
    schema: Schema,
    selection: Selection,
}

impl QueryIterator {
    pub fn create(
        selection: Selection,
        plan: PlanStep,
        schema: Schema
    ) -> QueryIterator {
        QueryIterator { source: plan, schema, selection }
    }

    pub fn get_selected_columns(&self) -> Vec<Column> {
        match &self.selection {
            Selection::All => {
                self.schema.get_columns()
            },
            Selection::Some(selected_columns_names) => {
                let mut selected_columns = Vec::new();

                for selected_columns_name in selected_columns_names {
                    let column = self.schema.get_column(selected_columns_name)
                        .unwrap();
                    selected_columns.push(column);
                }

                selected_columns
            }
        }
    }

    pub fn next_n(&mut self, n: usize) -> Result<Vec<Row>, SimpleDbError> {
        let mut results = Vec::new();

        while results.len() <= n {
            match self.source.next()? {
                Some(row) => results.push(row),
                None => break
            };
        }

        Ok(results)
    }

    pub fn all(&mut self) -> Result<Vec<Row>, SimpleDbError> {
        let mut results = Vec::new();

        while let Some(row) = self.source.next()? {
            results.push(row);
        }

        Ok(results)
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_plan_desc(&self) -> PlanStepDesc {
        self.source.desc()
    }
}

impl RowIterator for QueryIterator {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        self.source.next()
    }
}