use crate::table::record::{Record, RecordBuilder};
use crate::table::row::Row;
use crate::table::table::Table;
use bytes::Bytes;
use shared::iterators::storage_iterator::StorageIterator;
use shared::{ColumnId, Value};
use std::sync::Arc;
use crate::selection::Selection;

//This is the iterator that will be exposed to users of the SimpleDB
//As data is stored in LSM engine, which is "only" append only, update records of rows are stored as ID -> [updated column id, new column value]
//So if we want to get all the columns of a row existing row by its ID, we will need to reassemble it because column values will
//scatter over sstables and memtables.
#[derive(Clone)]
struct RowReassemble {
    is_fully_reassembled: bool,
    record_builder: RecordBuilder,
    key: Bytes,

    selection: Vec<ColumnId>
}

#[derive(Clone)]
pub struct TableIterator<I: StorageIterator> {
    simple_db_storage_iterator: I,
    selection: Vec<ColumnId>, //Columns ID to retrieve from storage engine

    rows_reassembling: Vec<RowReassemble>,
    current_row: Option<Row>,

    table: Arc<Table>
}

impl<I: StorageIterator> TableIterator<I> {
    pub(crate) fn create(
        simple_db_storage_iterator: I,
        selection: Vec<ColumnId>, //Columns ID to select
        table: Arc<Table>
    ) -> TableIterator<I> {
        TableIterator {
            rows_reassembling: Vec::new(),
            simple_db_storage_iterator,
            current_row: None,
            selection,
            table,
        }
    }

    pub fn next(&mut self) -> bool {
        while self.n_reassembled_rows_that_can_be_returned() == 0 {
            if !self.simple_db_storage_iterator.next() {
                break;
            }

            let record = Record::deserialize(self.simple_db_storage_iterator.value().to_vec());
            let key = Bytes::copy_from_slice(self.simple_db_storage_iterator.key().as_bytes());
            self.reassemble_row(key, record);
        }

        if self.rows_reassembling.is_empty() {
            return false;
        }

        let row_in_reassembling = self.rows_reassembling.remove(0);
        let key_bytes = row_in_reassembling.key.clone();
        let mut row_record_reassembled = row_in_reassembling.build();
        row_record_reassembled.project_selection(&self.selection);

        let schema = self.table.get_schema();

        self.current_row = Some(Row::create(
            row_record_reassembled,
            Value::create(key_bytes, schema.get_primary_column().column_type).unwrap(),
            self.table.get_schema().clone())
        );

        true
    }

    pub fn row(&self) -> &Row {
        self.current_row.as_ref().unwrap()
    }

    fn reassemble_row(&mut self, key: Bytes, record: Record) {
        let schema = self.table.get_schema();

        let row_reassemble_index = match self.find_row_reassemble_index(&key) {
            Some(row_reassemble_index) => row_reassemble_index,
            None => {
                let mut record_builder = Record::builder();
                record_builder.add_column(schema.get_primary_column().column_id, key.clone());

                self.rows_reassembling.push(RowReassemble {
                    selection: self.selection.clone(),
                    is_fully_reassembled: false,
                    record_builder,
                    key,
                });
                self.rows_reassembling.len() - 1
            }
        };

        let row_reassemble = &mut self.rows_reassembling[row_reassemble_index];
        row_reassemble.add_record(record);
    }
    
    fn find_row_reassemble_index(&self, key: &Bytes) -> Option<usize> {
        for (current_index, current_row_reassemble) in self.rows_reassembling.iter().enumerate() {
            if current_row_reassemble.key.eq(key) {
                return Some(current_index);
            }
        }

        None
    }

    fn n_reassembled_rows_that_can_be_returned(&self) -> usize {
        let mut n_reassembled_rows = 0;
        for row_reassembling in &self.rows_reassembling {
            if row_reassembling.is_fully_reassembled {
                n_reassembled_rows = n_reassembled_rows + 1;
            } else {
                break
            }
        }

        n_reassembled_rows
    }
}

impl RowReassemble {
    pub fn add_record(&mut self, record: Record) {
        self.record_builder.add_record(record);
        if self.record_builder.has_columns_id(self.selection.as_ref()) {
            self.is_fully_reassembled = true;
        }
    }

    pub fn build(self) -> Record {
        self.record_builder.build()
    }
}

#[cfg(test)]
mod test {
    use crate::table::record::Record;
    use crate::table::table::Table;
    use crate::table::table_iterator::TableIterator;
    use bytes::Bytes;
    use shared::iterators::mock_iterator::MockIterator;
    use shared::{ColumnId, Type, Value};
    use crate::table::schema::Column;
    //Given records:
    //1 -> (1, 100)
    //1 -> (1, 90), (2, Pago)
    //2 -> (1, 200), (3, 30/30/3000)
    //2 -> (2, Cena)
    //3 -> (1, 350)
    //4 -> (2, Pepita), (3, 1999)
    //Columns ID to get: [1, 2]

    //The iterator should return:
    // 1 -> (1, 100),  (2, Pago)
    // 2 -> (1, 200),  (2, Cena)
    // 3 -> (1, 350),  (2, NULL)
    // 4 -> (1, NULL), (2, Pepita)
    #[test]
    fn iterator() {
        let mut iterator = TableIterator::create(
            MockIterator::create_from_byte_entries(vec![
                (1, record(vec![(2, "100")])),
                (1, record(vec![(2, "90"), (3, "Pago")])),
                (2, record(vec![(2, "200"), (4, "30/30/3000")])),
                (2, record(vec![(3, "Cena")])),
                (3, record(vec![(2, "350")])),
                (4, record(vec![(3, "Pepita"), (4, "1999")])),
            ]),
            vec![2, 3],
            Table::create_mock(vec![
                Column{column_id: 1, column_type: Type::I64, column_name: String::from("ID"), is_primary: true, secondary_index_keyspace_id: None },
                Column{column_id: 2, column_type: Type::String, column_name: String::from("Money"), is_primary: false, secondary_index_keyspace_id: None },
                Column{column_id: 3, column_type: Type::String, column_name: String::from("Desc"), is_primary: false, secondary_index_keyspace_id: None },
                Column{column_id: 4, column_type: Type::String, column_name: String::from("Fecha"), is_primary: false, secondary_index_keyspace_id: None },
            ])
        );

        assert!(iterator.next());

        //Row 1º
        let row1 = iterator.row();
        let id = row1.get_column_value("ID").unwrap().get_i64().unwrap();
        assert_eq!(id, 1);
        let money = row1.get_column_value("Money").unwrap();
        let money = money.get_string().unwrap();
        assert_eq!(money, "100");
        let desc = row1.get_column_value("Desc").unwrap();
        let desc = desc.get_string().unwrap();
        assert_eq!(desc, "Pago");

        //Row 2º
        assert!(iterator.next());
        let row2 = iterator.row();
        let id = row2.get_column_value("ID").unwrap().get_i64().unwrap();
        assert_eq!(id, 2);
        let money = row2.get_column_value("Money").unwrap();
        let money = money.get_string().unwrap();
        assert_eq!(money, "200");
        let desc = row2.get_column_value("Desc").unwrap();
        let desc = desc.get_string().unwrap();
        assert_eq!(desc, "Cena");

        //Row 3º
        assert!(iterator.next());
        let row3 = iterator.row();
        let id = row3.get_column_value("ID").unwrap().get_i64().unwrap();
        assert_eq!(id, 3);
        let money = row3.get_column_value("Money").unwrap();
        let money = money.get_string().unwrap();
        assert_eq!(money, "350");
        let desc = row3.get_column_value("Desc").unwrap();
        assert!(desc.is_null());

        //Row 4º
        assert!(iterator.next());
        let row4 = iterator.row();
        let id = row4.get_column_value("ID").unwrap().get_i64().unwrap();
        assert_eq!(id, 4);
        let money = row4.get_column_value("Money").unwrap();
        assert!(desc.is_null());
        let desc = row4.get_column_value("Desc").unwrap();
        let desc = desc.get_string().unwrap();
        assert_eq!(desc, "Pepita");

        assert!(!iterator.next());
    }

    fn record(rows: Vec<(i32, &str)>) -> Bytes {
        let mut record_builder = Record::builder();
        for (column_id, column_value) in rows {
            record_builder.add_column(column_id as ColumnId, Bytes::from(column_value.to_string()));
        }

        let record = record_builder.build();
        let serialized = record.serialize();
        Bytes::from(serialized)
    }
}