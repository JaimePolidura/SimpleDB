use crate::sql::expression::Expression;
use bytes::Bytes;
use shared::SimpleDbError;
use SimpleDbError::MalformedQuery;

#[derive(Debug, Clone, PartialEq)]
pub enum ScanType {
    Full,

    //Expression: Should produce the literal value which will be the primary key
    ExactPrimary(Expression),
    //String: Should
    //Expression: Should produce the literal value which will be the secondary key
    ExactSecondary(String, Expression),

    MergeUnion(Box<ScanType>, Box<ScanType>),
    MergeIntersection(Box<ScanType>, Box<ScanType>),

    // min < values < expression
    Range(RangeScan),
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeScan {
    pub column_name: String,
    pub start: Option<Expression>,
    pub start_inclusive: bool,
    pub end: Option<Expression>,
    pub end_inclusive: bool,
}

pub enum RangeKeyPosition {
    Bellow,
    Inside,
    Above
}

impl RangeScan {
    // WHERE id > 100 AND id < 200 -> Range(100, _) AND Range(_, 200) = Range(100, 200)
    // WHERE id > 200 AND id < 100 -> Range(200, _) AND Range(_, 100) = Range(_, _) Invalid!
    // WHERE id > 100 AND id > 200 -> Range(100, _) AND Range(200, _) = Range(200, _)
    // WHERE id < 100 AND id < 200 -> Range(_, 100) AND Range(_, 200) = Range(_, 100)
    pub fn and(&self, other: RangeScan) -> Result<RangeScan, SimpleDbError> {
        if self.has_only_start() && other.has_only_end() {
            let range_scan = RangeScan {
                column_name: other.column_name,
                start: self.start.clone(),
                start_inclusive: self.start_inclusive,
                end: other.end.clone(),
                end_inclusive: other.end_inclusive
            };
            range_scan.is_valid()?;
            return Ok(range_scan);
        }
        if self.has_only_end() && other.has_only_start() {
            let range_scan = RangeScan {
                column_name: other.column_name,
                start: other.start.clone(),
                start_inclusive: other.start_inclusive,
                end: self.end.clone(),
                end_inclusive: self.end_inclusive
            };
            range_scan.is_valid()?;
            return Ok(range_scan);
        }
        let mut result = RangeScan::empty();
        result.column_name = self.column_name.clone();
        if self.has_start() && other.has_start() {
            let start_self = self.start.clone().unwrap();
            let start_other = other.start.clone().unwrap();
            let is_self_less_than_other = start_self.le(&start_other)?.get_boolean()?;
            let is_new_start_inclusive = if is_self_less_than_other { other.start_inclusive } else { self.start_inclusive };
            let new_start = if is_self_less_than_other { start_other } else { start_self };

            result.start_inclusive = is_new_start_inclusive;
            result.start = Some(new_start);
        }
        if self.has_end() && other.has_end() {
            let end_other = other.end.clone().unwrap();
            let end_self = self.end.clone().unwrap();
            let is_self_less_than_other = end_self.le(&end_other)?.get_boolean()?;
            let is_new_end_inclusive = if is_self_less_than_other { self.end_inclusive } else { other.end_inclusive };
            let new_end = if is_self_less_than_other { end_self } else { end_other };

            result.end_inclusive = is_new_end_inclusive;
            result.end = Some(new_end);
        }

        result.is_valid()?;
        Ok(result)
    }

    fn is_valid(&self) -> Result<(), SimpleDbError> {
        if self.has_start() && self.has_end() {
            let start = self.start.as_ref().unwrap();
            let end = self.end.as_ref().unwrap();

            if start.ge(end)?.get_boolean()? {
                return Err(MalformedQuery(String::from("Invalid range")))
            }
        }

        Ok(())
    }

    pub fn same_column(&self, other: &RangeScan) -> bool {
        self.column_name.eq(&other.column_name)
    }

    pub fn get_position(&self, other_key: &Bytes) -> RangeKeyPosition {
        if let Some(start_key) = self.start.as_ref() {
            let start_key_bytes = start_key.get_literal_bytes();
            let is_bellow = (self.start_inclusive && start_key_bytes.gt(other_key)) ||
                (!self.start_inclusive && start_key_bytes.ge(other_key));
            if is_bellow {
                return RangeKeyPosition::Bellow;
            }
        }
        if let Some(end_key) = self.end.as_ref() {
            let end_key_bytes = end_key.get_literal_bytes();
            let is_above = (self.end_inclusive && end_key_bytes.lt(other_key)) ||
                (!self.end_inclusive && end_key_bytes.le(other_key));
            if is_above {
                return RangeKeyPosition::Above;
            }
        }

        RangeKeyPosition::Inside
    }

    pub fn is_start_inclusive(&self) -> bool {
        self.start_inclusive
    }

    pub fn has_end(&self) -> bool {
        self.end.is_some()
    }

    pub fn has_start(&self) -> bool {
        self.start.is_some()
    }

    pub fn has_only_start(&self) -> bool {
        self.start.is_some() && self.end.is_none()
    }

    pub fn has_only_end(&self) -> bool {
        self.start.is_none() && self.end.is_some()
    }

    pub fn start(&self) -> Option<&Expression> {
        self.start.as_ref()
    }

    pub fn empty() -> RangeScan {
        RangeScan {
            column_name: String::from(""),
            start: None,
            start_inclusive: false,
            end: None,
            end_inclusive: false,
        }
    }
}

#[cfg(test)]
mod test {
    use shared::Value;
    use crate::sql::expression::Expression;
    use crate::sql::plan::scan_type::RangeScan;

    //Range(_, 100] AND Range(_, 200) = Range(_, 100)
    #[test]
    fn and_5() {
        let a = RangeScan {
            column_name: String::from("a"),
            start_inclusive: false,
            start: None,
            end: Some(Expression::Literal(Value::create_i64(100))),
            end_inclusive: true,
        };
        let b = RangeScan {
            column_name: String::from("a"),
            start_inclusive: false,
            start: None,
            end: Some(Expression::Literal(Value::create_i64(200))),
            end_inclusive: false,
        };

        let result = a.and(b).unwrap();

        assert_eq!(result, RangeScan {
            column_name: String::from("a"),
            start: None,
            start_inclusive: false,
            end: Some(Expression::Literal(Value::create_i64(100))),
            end_inclusive: true,
        });
    }


    //Range(_, 100) AND Range(_, 200] = Range(_, 100)
    #[test]
    fn and_4() {
        let a = RangeScan {
            column_name: String::from("a"),
            start_inclusive: false,
            start: None,
            end: Some(Expression::Literal(Value::create_i64(100))),
            end_inclusive: false,
        };
        let b = RangeScan {
            column_name: String::from("a"),
            start_inclusive: false,
            start: None,
            end: Some(Expression::Literal(Value::create_i64(200))),
            end_inclusive: true,
        };

        let result = a.and(b).unwrap();

        assert_eq!(result, RangeScan {
            column_name: String::from("a"),
            start: None,
            start_inclusive: false,
            end: Some(Expression::Literal(Value::create_i64(100))),
            end_inclusive: false,
        });
    }

    //Range(100, _) AND Range [200, _) = Range(200, _)
    #[test]
    fn and_3() {
        let a = RangeScan {
            column_name: String::from("a"),
            start: Some(Expression::Literal(Value::create_i64(100))),
            start_inclusive: false,
            end: None,
            end_inclusive: false,
        };
        let b = RangeScan {
            column_name: String::from("a"),
            start: Some(Expression::Literal(Value::create_i64(200))),
            start_inclusive: true,
            end: None,
            end_inclusive: false,
        };

        let result = a.and(b).unwrap();

        assert_eq!(result, RangeScan {
            column_name: String::from("a"),
            start: Some(Expression::Literal(Value::create_i64(200))),
            start_inclusive: true,
            end: None,
            end_inclusive: false,
        });
    }

    //Range(200, _) AND Range(_, 100) = Range(_, _) Invalid!
    #[test]
    fn and_2() {
        let a = RangeScan {
            column_name: String::from("a"),
            start: Some(Expression::Literal(Value::create_i64(200))),
            start_inclusive: false,
            end: None,
            end_inclusive: false,
        };
        let b = RangeScan {
            column_name: String::from("a"),
            start: None,
            start_inclusive: false,
            end: Some(Expression::Literal(Value::create_i64(100))),
            end_inclusive: false,
        };

        let result = a.and(b);
        assert!(result.is_err());
    }

    //Range(100, _) AND Range(_, 200) = Range(100, 200)
    #[test]
    fn and_1() {
        let a = RangeScan {
            column_name: String::from("a"),
            start: Some(Expression::Literal(Value::create_i64(100))),
            start_inclusive: false,
            end: None,
            end_inclusive: false,
        };
        let b = RangeScan {
            column_name: String::from("a"),
            start: None,
            start_inclusive: false,
            end: Some(Expression::Literal(Value::create_i64(200))),
            end_inclusive: false,
        };

        let result = a.and(b).unwrap();

        assert_eq!(result, RangeScan {
            column_name: String::from("a"),
            start: Some(Expression::Literal(Value::create_i64(100))),
            start_inclusive: false,
            end: Some(Expression::Literal(Value::create_i64(200))),
            end_inclusive: false,
        });
    }
}