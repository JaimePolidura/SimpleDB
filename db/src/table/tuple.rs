use bytes::{BufMut, Bytes};
use shared::ColumnId;

pub struct Tuple {
    pub(crate) id: Bytes,
    pub(crate) data_records: Vec<(ColumnId, Bytes)>
}

impl Tuple {
    pub fn serialize(mut self) -> Vec<u8> {
        let mut result = Vec::new();

        while let Some((column_id, column_value)) = self.data_records.pop() {
            result.put_u16_le(column_id);
            result.put_u32_le(column_value.len() as u32);
            result.extend(column_value);
        }

        result
    }
}