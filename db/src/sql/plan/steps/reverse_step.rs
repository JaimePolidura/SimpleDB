use std::sync::Arc;
use bytes::{BufMut, Bytes};
use shared::{utils, SimpleDbError, SimpleDbFileMode};
use shared::SimpleDbError::CannotReadReverseFile;
use storage::TemporarySpace;
use crate::{PlanStepDesc, Row};
use crate::sql::plan::plan_step::{PlanStep, PlanStepTrait};
use crate::table::row::RowIterator;
use crate::table::table::Table;

#[derive(Clone)]
pub struct ReverseStep {
    pub(crate) source: PlanStep,
    pub(crate) table: Arc<Table>,
    pub(crate) state: RevserseStepState,
    pub(crate) temporary_space: TemporarySpace,
}

#[derive(Clone)]
enum RevserseStepState {
    Reversing,
    //Last offset read
    Reversed(usize)
}

impl ReverseStep {
    pub fn create(source: PlanStep, table: Arc<Table>) -> Result<ReverseStep, SimpleDbError> {
        Ok(ReverseStep {
            temporary_space: table.storage.create_temporary_space()?,
            state: RevserseStepState::Reversing,
            source,
            table
        })
    }
}

impl PlanStepTrait for ReverseStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match self.state.clone() {
            RevserseStepState::Reversing => {
                let mut reverse_file = self.temporary_space.create_file("reversed", SimpleDbFileMode::AppendOnly)?;

                while let Some(row) = self.source.next()? {
                    let row_serialized = row.serialize();
                    let mut serialized: Vec<u8> = Vec::new();
                    serialized.put_u32_le(row_serialized.len() as u32);
                    serialized.extend(row_serialized);

                    reverse_file.write(&serialized)
                        .map_err(|e| SimpleDbError::CannotWriteReverseFile(e))?;
                }

                self.state = RevserseStepState::Reversed(0)
            }
            RevserseStepState::Reversed(last_offset_read) => {
                let revsersed_file = self.temporary_space.get_file("revsersed", SimpleDbFileMode::ReadOnly)?;

                if last_offset_read >= revsersed_file.size() {
                    return Ok(None);
                }

                let row_size_bytes = utils::bytes_to_u32_le(&Bytes::from(revsersed_file.read(last_offset_read, 4)
                    .map_err(|e| CannotReadReverseFile(e))?)) as usize;
                let row_bytes = revsersed_file.read(last_offset_read + 4, row_size_bytes)
                    .map_err(|e| CannotReadReverseFile(e))?;
                let row = Row::deserialize(&mut row_bytes.as_slice(), self.table.get_schema());

                self.state = RevserseStepState::Reversed(last_offset_read + 4 + row_size_bytes);

                return Ok(Some(row));
            }
        };

        Ok(None)
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::Revserse(Box::new(self.source.desc()))
    }
}