use bytes::BufMut;
use db::{Column, IndexType, Limit, PlanStepDesc, QueryIterator, RangeScan, Row, Type, Value};
use shared::{ErrorTypeId, SimpleDbError};

pub enum Response {
    Statement(StatementResponse),
    Error(ErrorTypeId), //Error number
    Ok,
}

pub enum StatementResponse {
    Ok(usize),
    Rows(RowsResponse),
    Databases(Vec<String>),
    Tables(Vec<String>),
    Indexes(Vec<(String, IndexType)>),
    Describe(Vec<Column>),
    Explanation(QueryIterator)
}

pub struct RowsResponse {
    columns_desc: Vec<Column>,
    rows: Vec<Row>,
}

impl Response {
    pub fn from_simpledb_error(error: SimpleDbError) -> Response {
        Response::Error(error.serialize())
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();

        serialized.put_u8(self.message_type_id());
        serialized.extend(self.serialize_message_content());

        serialized
    }

    fn serialize_message_content(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        match self {
            Response::Statement(result) => serialized.extend(result.serialize()),
            Response::Error(error_type_id) => serialized.put_u8(*error_type_id as u8),
            Response::Ok => {},
        };

        serialized
    }

    fn message_type_id(&self) -> u8 {
        match self {
            Response::Statement(_) => 1,
            Response::Error(_) => 2,
            Response::Ok => 3
        }
    }
}

impl RowsResponse {
    pub fn create(
        columns_desc: Vec<Column>,
        rows: Vec<Row>
    ) -> RowsResponse {
        RowsResponse { columns_desc, rows }
    }

    pub fn get_primary_column_name(&self) -> &str {
        for column_desc in &self.columns_desc {
            if column_desc.is_primary {
                return &column_desc.column_name;
            }
        }

        panic!("No primary column found")
    }

    pub fn get_column_type(&self, column_name: &str) -> Type {
        for column_desc in &self.columns_desc {
            if column_desc.column_name == column_name {
                return column_desc.column_type.clone();
            }
        }

        panic!("No column found with that name found")
    }
}

impl StatementResponse {
    pub fn serialize(&self) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.put_u8(self.statement_response_type_id());

        match self {
            StatementResponse::Describe(columns_desc) => serialized.extend(Self::serialize_columns_desc(columns_desc)),
            StatementResponse::Explanation(explanation) => serialized.extend(Self::serialize_explanation(explanation)),
            StatementResponse::Databases(databases) => serialized.extend(Self::serialize_string_vec(databases)),
            StatementResponse::Indexes(indexes) => serialized.extend(Self::serialize_show_indexes(indexes)),
            StatementResponse::Rows(data) => serialized.extend(Self::serialize_query_data(data)),
            StatementResponse::Tables(tables) => serialized.extend(Self::serialize_string_vec(tables)),
            StatementResponse::Ok(n_affected_rows) => serialized.put_u64_le(*n_affected_rows as u64),
        };

        serialized
    }

    fn serialize_show_indexes(
        indexes: &Vec<(String, IndexType)>
    ) -> Vec<u8> {
        let mut serialized: Vec<u8> = Vec::new();

        serialized.put_u32_le(indexes.len() as u32);
        for (indexed_column_name, index_type) in indexes {
            serialized.put_u32_le(indexed_column_name.len() as u32);
            serialized.extend(indexed_column_name.as_bytes());
            serialized.put_u8(index_type.serialize());
        }

        serialized
    }

    fn serialize_query_data(
        query_data_response: &RowsResponse,
    ) -> Vec<u8> {
        let mut serialized = Vec::new();

        //Columns desc
        serialized.extend(Self::serialize_columns_desc(&query_data_response.columns_desc));
        //Rows
        serialized.put_u32_le(query_data_response.rows.len() as u32);
        for row in &query_data_response.rows {
            serialized.extend(row.clone().serialize());
        }

        serialized
    }

    fn serialize_columns_desc(
        columns_desc: &Vec<Column>
    ) -> Vec<u8> {
        let mut serialized = Vec::new();

        serialized.put_u32_le(columns_desc.len() as u32);
        for columns_desc in columns_desc {
            serialized.extend(columns_desc.serialize());
        }

        serialized
    }

    fn serialize_string_vec(strings: &Vec<String>) -> Vec<u8> {
        let mut serialized = Vec::new();
        serialized.put_u32_le(strings.len() as u32);
        for string in strings {
            serialized.put_u32_le(string.len() as u32);
            serialized.extend(string.bytes());
        }

        serialized
    }

    fn statement_response_type_id(&self) -> u8 {
        match self {
            StatementResponse::Ok(_) => 1,
            StatementResponse::Rows(_) => 2,
            StatementResponse::Databases(_) => 3,
            StatementResponse::Tables(_) => 4,
            StatementResponse::Describe(_) => 5,
            StatementResponse::Indexes(_) => 6,
            StatementResponse::Explanation(_) => 7
        }
    }

    fn serialize_explanation(
        explanation: &QueryIterator,
    ) -> Vec<u8> {
        let plan = explanation.get_plan_desc();
        let schema = explanation.schema();

        //Since the plan steps desc follows a tre structure, it wil be very complicated to create,
        //a binary format for it. Instead, we will convert it to a string which will be displayed by the cli.
        let mut pending = Vec::new();
        // let mut strings = Vec::new();
        pending.push((0, explanation.clone()));

        // while let Some((depth, current_step)) = pending.pop() {
        //     match current_step {
        //         PlanStepDesc::Limit(limit, source) => {
        //             pending.push((depth, source.clone()));
        //             strings.push(Self::limit_plan_desc_to_string(depth, limit));
        //         }
        //         PlanStepDesc::Filter(source) => {
        //             pending.push((depth, source.clone()));
        //             strings.push(Self::filter_plan_desc_to_string(depth));
        //         },
        //         PlanStepDesc::MergeIntersection(left, right) => {
        //             pending.push((depth + 1, left.clone()));
        //             pending.push((depth + 1, right.clone()));
        //             strings.push(Self::intersection_plan_desc_to_string(depth));
        //         }
        //         PlanStepDesc::MergeUnion(left, right) => {
        //             pending.push((depth + 1, left.clone()));
        //             pending.push((depth + 1, right.clone()));
        //             strings.push(Self::union_plan_desc_to_string(depth));
        //         }
        //         PlanStepDesc::FullScan => {
        //             strings.push(Self::full_scan_to_string(depth));
        //         }
        //         PlanStepDesc::RangeScan(range) => {
        //             strings.push(Self::range_scan_plan_desc_to_string(depth, range));
        //         }
        //         PlanStepDesc::PrimaryExactScan(primary_column_value_bytes) => {
        //             let primary_column = schema.get_primary_column();
        //             let primary_column_type = primary_column.column_type;
        //             let primary_column_value = Value::deserialize(primary_column_value_bytes.clone(), primary_column_type)
        //                 .unwrap();
        //
        //             strings.push(Self::exact_primary_scan_plan_desc_to_string(depth, primary_column_value));
        //         }
        //         PlanStepDesc::SecondaryExactExactScan(secondary_column_name, secondary_column_value) => {
        //             let secondary_column = schema.get_column_or_err(&secondary_column_name).unwrap();
        //             let secondary_column_type = secondary_column.column_type;
        //             let secondary_column_value = Value::deserialize(secondary_column_value.clone(), secondary_column_type)
        //                 .unwrap();
        //             strings.push(Self::exact_secondary_scan_plan_desc_to_string(depth, secondary_column_name, secondary_column_value));
        //         }
        //     };
        // }

        vec![]
    }

    fn limit_plan_desc_to_string(
        depth: usize,
        limit: &Limit
    ) -> String {
        let mut string = Self::explain_plan_new_line(depth);
        match limit {
            Limit::None => {
                string.push_str("Limit (None)");
            }
            Limit::Some(limit_n) => {
                string.push_str(&format!("Limit ({})", limit_n));
            }
        };
        string
    }

    fn full_scan_to_string(depth: usize) -> String {
        let mut string = Self::explain_plan_new_line(depth);
        string.push_str("Full Scan");
        string
    }

    fn filter_plan_desc_to_string(depth: usize) -> String {
        let mut string = Self::explain_plan_new_line(depth);
        string.push_str("Filter (Query)");
        string
    }

    fn union_plan_desc_to_string(depth: usize) -> String {
        let mut string = Self::explain_plan_new_line(depth);
        string.push_str("Union");
        string
    }

    fn intersection_plan_desc_to_string(depth: usize) -> String {
        let mut string = Self::explain_plan_new_line(depth);
        string.push_str("Intersection");
        string
    }

    fn range_scan_plan_desc_to_string(depth: usize, range: &RangeScan) -> String {
        let mut string = Self::explain_plan_new_line(depth);
        string.push_str("Range ");
        string.push_str(&range.column_name);
        string.push_str(" ");
        if range.start_inclusive {
            string.push_str("[");
        } else {
            string.push_str("(");
        }

        if let Some(start) = &range.start {
            string.push_str(&start.get_value().unwrap().to_string());
        } else {
            string.push_str("_");
        }

        string.push_str(", ");

        if let Some(end) = &range.end {
            string.push_str(&end.get_value().unwrap().to_string());
        } else {
            string.push_str("_");
        }

        if range.end_inclusive {
            string.push_str("]");
        } else {
            string.push_str(")");
        }

        string
    }

    fn exact_primary_scan_plan_desc_to_string(depth: usize, primary_value: Value) -> String {
        let mut string = Self::explain_plan_new_line(depth);
        string.push_str("Exact primary (");
        string.push_str(&primary_value.to_string());
        string.push_str(")");
        string
    }

    fn exact_secondary_scan_plan_desc_to_string(
        depth: usize,
        secondary_column_name: &String,
        secondary_column_value: Value
    ) -> String {
        let mut string = Self::explain_plan_new_line(depth);
        string.push_str("Exact secondary ");
        string.push_str(secondary_column_name);
        string.push_str(" (");
        string.push_str(&secondary_column_value.to_string());
        string.push_str(")");
        string
    }

    fn explain_plan_new_line(depth: usize) -> String {
        let mut string = String::new();
        string.push_str(&" ".repeat(depth));
        string
    }
}