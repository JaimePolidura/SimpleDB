use bytes::{Buf, BufMut, Bytes};
use shared::ColumnId;

pub struct Tuple {
    pub(crate) data_records: Vec<(ColumnId, Bytes)>
}

impl Tuple {
    //Missing records from other will be added
    //Repeated records will be replaced by other
    pub fn merge(&mut self, mut other: Tuple) {
        while let Some((other_column_id, other_column_value)) = other.data_records.pop() {
            match self.get_column_id(other_column_id) {
                Some(self_column_id_index) => {
                    self.data_records[self_column_id_index] = (other_column_id, other_column_value);
                },
                None => {
                    self.data_records.push((other_column_id, other_column_value));
                }
            }
        }
    }

    fn get_column_id_index(&self, column_id_lookup: ColumnId) -> Option<usize> {
        let mut current_column_index = 0;

        for (column_id, _) in &self.data_records {
            if *column_id == column_id_lookup {
                return Some(current_column_index);
            }

            current_column_index = current_column_index + 1;
        }

        None
    }

    pub fn serialize(mut self) -> Vec<u8> {
        let mut result = Vec::new();

        while let Some((column_id, column_value)) = self.data_records.pop() {
            result.put_u16_le(column_id);
            result.put_u32_le(column_value.len() as u32);
            result.extend(column_value);
        }

        result
    }

    pub fn deserialize(bytes: Vec<u8>) -> Tuple {
        let mut current_ptr = bytes.as_slice();
        let mut data_records: Vec<(ColumnId, Bytes)> = Vec::new();

        while current_ptr.has_remaining() {
            let column_id = current_ptr.get_u16_le() as ColumnId;
            let column_value_length = current_ptr.get_u32_le();
            let column_value_bytes = &current_ptr[..column_value_length as usize];

            data_records.push((column_id, Bytes::from(column_value_bytes.to_vec())));
        }

        Tuple { data_records }
    }
}