use shared::SimpleDbError;
use crate::Row;

pub trait PlanStep {
    fn next(&mut self) -> Result<Option<&Row>, SimpleDbError>;
}
