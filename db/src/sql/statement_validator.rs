use std::sync::Arc;
use shared::{SimpleDbError, SimpleDbOptions};
use crate::Database;
use crate::sql::statement::Statement;

pub struct StatementValidator {
    options: Arc<SimpleDbOptions>
}

impl StatementValidator {
    pub fn create(
        options: &Arc<SimpleDbOptions>
    ) -> StatementValidator {
        StatementValidator { options: options.clone() }
    }

    pub fn validate(
        &self,
        database: &Arc<Database>,
        statement: &Statement,
    ) -> Result<(), SimpleDbError> {
        todo!()
    }
}
