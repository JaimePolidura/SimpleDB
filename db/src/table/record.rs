use std::collections::HashSet;
use bytes::{Buf, BufMut, Bytes};
use shared::ColumnId;

//Represents the row data stored in the storage engine,
//This might represent an incomplete set of data
// Column ID (u16) | Column value length (u32) | Column value bytes |
#[derive(Clone)]
pub struct Record {
    pub(crate) data_records: Vec<(ColumnId, Bytes)>
}

impl Record {
    pub fn create(data_records: Vec<(ColumnId, Bytes)>) -> Record {
        Record { data_records }
    }

    //Missing records from other will be added
    //Repeated records will be replaced by other
    pub fn merge(&mut self, mut other: Record) {
        while let Some((other_column_id, other_column_value)) = other.data_records.pop() {
            match self.get_column_id_index(other_column_id) {
                Some(self_column_id_index) => {
                    self.data_records[self_column_id_index] = (other_column_id, other_column_value);
                },
                None => {
                    self.data_records.push((other_column_id, other_column_value));
                }
            }
        }
    }

    pub fn get_n_columns(&self) -> usize {
        self.data_records.len()
    }

    pub fn get_column_bytes(&self, column_id_lookup: ColumnId) -> Option<&Bytes> {
        for (current_column_id, current_column_value) in &self.data_records {
            if *current_column_id == column_id_lookup {
                return Some(current_column_value);
            }
        }

        None
    }

    pub fn project_selection(&mut self, selection_columns_id: &Vec<ColumnId>) {
        let selection_columns_id: HashSet<ColumnId> = selection_columns_id.clone().into_iter().collect();
        let mut columns_id_to_remove = Vec::new();

        for (column_id_from_row, _) in &self.data_records {
            if !selection_columns_id.contains(&column_id_from_row) {
                columns_id_to_remove.push(*column_id_from_row);
            }
        }

        for column_id_to_remove in columns_id_to_remove {
            self.remove_column(column_id_to_remove);
        }
    }

    pub fn remove_column(&mut self, column_id_lookup: ColumnId) -> Option<Bytes> {
        for (current_index, current_entry) in self.data_records.iter().enumerate() {
            let (current_column_id, _) = current_entry;

            if *current_column_id == column_id_lookup {
                let (_, value) =  self.data_records.remove(current_index);
                return Some(value);
            }
        }

        None
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

    pub fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();
        for (column_id, column_value) in &self.data_records {
            result.put_u16_le(*column_id);
            result.put_u32_le(column_value.len() as u32);
            result.extend(column_value);
        }

        result
    }

    pub fn deserialize(bytes: Vec<u8>) -> Record {
        let mut current_ptr = bytes.as_slice();
        let mut data_records: Vec<(ColumnId, Bytes)> = Vec::new();

        while current_ptr.has_remaining() {
            let column_id = current_ptr.get_u16_le() as ColumnId;
            let column_value_length = current_ptr.get_u32_le();
            let column_value_bytes = &current_ptr[..column_value_length as usize];
            current_ptr.advance(column_value_length as usize);

            data_records.push((column_id, Bytes::from(column_value_bytes.to_vec())));
        }

        Record { data_records }
    }

    pub fn builder() -> RecordBuilder {
        RecordBuilder { data_records: Vec::new() }
    }
}

 #[derive(Clone)]
pub struct RecordBuilder {
    data_records: Vec<(ColumnId, Bytes)>
}

impl RecordBuilder {
    pub fn add_record(&mut self, mut other: Record) {
        while let Some((column_id, other_value)) = other.data_records.pop() {
            self.add_column(column_id, other_value);
        }
    }

    //Adds the column if it doest exists
    //Returns true if the column was added, false if it wasn't added
    pub fn add_column(&mut self, column_id: ColumnId, column_value: Bytes) -> bool {
        if self.has_column_id(column_id) {
            return false
        }

        self.data_records.push((column_id, column_value));
        true
    }

    pub fn has_columns_id(&self, columns_ids: &Vec<ColumnId>) -> bool {
        for column_id in columns_ids {
            if !self.has_column_id(*column_id) {
                return false
            }
        }

        true
    }

    pub fn has_column_id(&self, column_id_lookup: ColumnId) -> bool {
        for (current_column_id, _) in &self.data_records {
            if *current_column_id == column_id_lookup{
                return true;
            }
        }

        false
    }

    pub fn build(self) -> Record {
        Record { data_records: self.data_records }
    }
}