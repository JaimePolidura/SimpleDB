use shared::SimpleDbError;
use crate::Row;

pub type Plan = Box<dyn PlanStep>;

pub trait PlanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError>;
}
