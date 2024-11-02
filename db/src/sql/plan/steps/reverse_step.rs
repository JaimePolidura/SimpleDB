use std::sync::Arc;
use bytes::{BufMut, Bytes};
use shared::{utils, SimpleDbError, SimpleDbFile, SimpleDbFileMode};
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
    pub(crate) reserved_file: SimpleDbFile,
}

#[derive(Clone)]
enum RevserseStepState {
    Reversing,
    //Last offset read. It starts from the top of the file and grows to 0
    Reversed(usize)
}

impl ReverseStep {
    pub fn create(source: PlanStep, table: Arc<Table>) -> Result<ReverseStep, SimpleDbError> {
        let temporary_space = table.storage.create_temporary_space()?;
        let reversed_file = temporary_space.create_file("reversed", SimpleDbFileMode::AppendOnly)?;

        Ok(ReverseStep {
            state: RevserseStepState::Reversing,
            temporary_space,
            reserved_file: reversed_file,
            source,
            table
        })
    }

    fn next_row_from_reserved_file(&mut self, last_offset_read: usize) -> Result<Option<Row>, SimpleDbError> {
        if last_offset_read <= 0 {
            return Ok(None);
        }

        let row_size_offset = last_offset_read - 4;

        let row_size_bytes = utils::bytes_to_u32_le(&Bytes::from(self.reserved_file.read(row_size_offset, 4)
            .map_err(|e| CannotReadReverseFile(e))?)) as usize;
        let row_bytes_offset = row_size_offset - row_size_bytes;

        let row_bytes = self.reserved_file.read(row_bytes_offset, row_size_bytes)
            .map_err(|e| CannotReadReverseFile(e))?;
        let row = Row::deserialize(&mut row_bytes.as_slice(), self.table.get_schema());

        self.state = RevserseStepState::Reversed(row_bytes_offset);

        return Ok(Some(row));
    }
}

impl PlanStepTrait for ReverseStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match self.state.clone() {
            RevserseStepState::Reversing => {
                while let Some(row) = self.source.next()? {
                    let row_serialized = row.serialize();
                    let row_serialized_size = row_serialized.len() as u32;
                    let mut serialized: Vec<u8> = Vec::new();
                    serialized.extend(row_serialized);
                    serialized.put_u32_le(row_serialized_size);

                    self.reserved_file.write(&serialized)
                        .map_err(|e| SimpleDbError::CannotWriteReverseFile(e))?;
                }

                self.state = RevserseStepState::Reversed(self.reserved_file.size());

                self.next_row_from_reserved_file(self.reserved_file.size())
            }
            RevserseStepState::Reversed(last_offset_read) => {
                self.next_row_from_reserved_file(last_offset_read)
            }
        }
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::Revserse(Box::new(self.source.desc()))
    }
}