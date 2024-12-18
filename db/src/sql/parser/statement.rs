use std::cmp::Ordering;
use shared::{Type, Value};
use crate::{Row, Schema};
use crate::table::selection::Selection;
use crate::sql::parser::expression::Expression;

pub enum Statement {
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Insert(InsertStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
    CreateDatabase(String),
    Describe(String),
    StartTransaction,
    Rollback,
    Commit,
    ShowIndexes(String), //Table name
    ShowDatabases,
    ShowTables,
}

pub struct SelectStatement {
    pub(crate) explain: bool,
    pub(crate) selection: Selection,
    pub(crate) table_name: String,
    pub(crate) where_expr: Option<Expression>,
    pub(crate) sort: Option<Sort>,
    pub(crate) limit: Limit,
}

pub struct UpdateStatement {
    pub(crate) table_name: String,
    pub(crate) updated_values: Vec<(String, Expression)>,
    pub(crate) where_expr: Option<Expression>
}

pub struct DeleteStatement {
    pub(crate) table_name: String,
    pub(crate) where_expr: Option<Expression>,
    pub(crate) limit: Limit
}

pub struct InsertStatement {
    pub(crate) table_name: String,
    //Column name, Value, Value type
    pub(crate) values: Vec<(String, Value)>,
}

pub struct CreateIndexStatement {
    pub(crate) table_name: String,
    pub(crate) column_name: String,
    pub(crate) wait: bool,
}

pub struct CreateTableStatement {
    pub(crate) table_name: String,
    //Column name, Column type, is primary
    pub(crate) columns: Vec<(String, Type, bool)>
}

#[derive(Clone)]
pub enum Limit {
    None,
    Some(usize)
}

#[derive(Debug, Clone, PartialEq)]
pub struct Sort {
    pub column_name: String,
    pub order: SortOrder
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc, Desc
}

impl Sort {
    pub fn is_indexed(&self, schema: &Schema) -> bool {
        let column = schema.get_column(&self.column_name).unwrap();
        column.is_secondary_indexed() || column.is_primary
    }

    pub fn compare(&self, a: &Row, b: &Row) -> Ordering {
        let value_a = a.get_column_value(&self.column_name).unwrap();
        let value_b = b.get_column_value(&self.column_name).unwrap();

        match self.order {
            SortOrder::Desc => value_b.cmp(&value_a),
            SortOrder::Asc => value_a.cmp(&value_b)
        }
    }
}

enum Requirement {
    ObligatoryToNotHave,
    ObligatoryToHave,
    Optional,
}

pub struct StatementDescriptor {
    transaction_req: Requirement,
    database_req: Requirement,
    creates_transaction: bool,
    terminates_transaction: bool
}

impl StatementDescriptor {
    pub fn terminates_transaction(&self) -> bool {
        self.terminates_transaction
    }

    pub fn creates_transaction(&self) -> bool {
        self.creates_transaction
    }

    pub fn requires_transaction(&self) -> bool {
        match self.transaction_req {
            Requirement::ObligatoryToHave => true,
            Requirement::ObligatoryToNotHave => false,
            Requirement::Optional => false,
        }
    }

    pub fn requires_database(&self) -> bool {
        match self.database_req {
            Requirement::ObligatoryToHave => true,
            Requirement::ObligatoryToNotHave => false,
            Requirement::Optional => false,
        }
    }
}

impl UpdateStatement {
    pub fn get_updated_values(&self) -> Selection {
        let mut column_names = Vec::new();
        for (column_name, _) in &self.updated_values {
            column_names.push(column_name.clone());
        }
        if let Some(where_expr) = &self.where_expr {
            column_names.extend(where_expr.get_identifiers());
        }

        Selection::Some(column_names)
    }
}

impl Statement {
    pub fn terminates_transaction(&self) -> bool {
        match *self {
            Statement::Rollback | Statement::Commit => true,
            _ => false
        }
    }

    pub fn requires_transaction(&self) -> bool {
        self.get_descriptor().requires_transaction()
    }

    pub fn requires_database(&self) -> bool {
        self.get_descriptor().requires_database()
    }

    pub fn is_explained(&self) -> bool {
        match self {
            Statement::Select(select) => select.explain,
            _ => false,
        }
    }

    pub fn get_descriptor(&self) -> StatementDescriptor {
        match self {
            Statement::Select(_) => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::ObligatoryToHave,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::Update(_) => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::ObligatoryToHave,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::Delete(_) => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::ObligatoryToHave,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::Insert(_) => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::ObligatoryToHave,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::CreateTable(_) => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::Optional,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::CreateDatabase(_) => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::Optional,
                database_req: Requirement::ObligatoryToNotHave
            },
            Statement::Describe(_) => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::Optional,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::StartTransaction => StatementDescriptor {
                creates_transaction: true,
                terminates_transaction: false,
                transaction_req: Requirement::ObligatoryToNotHave,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::Rollback => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: true,
                transaction_req: Requirement::ObligatoryToHave,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::Commit => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: true,
                transaction_req: Requirement::ObligatoryToHave,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::ShowDatabases => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::Optional,
                database_req: Requirement::ObligatoryToNotHave
            },
            Statement::ShowTables => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::Optional,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::CreateIndex(_) => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::Optional,
                database_req: Requirement::ObligatoryToHave
            },
            Statement::ShowIndexes(_) => StatementDescriptor {
                creates_transaction: false,
                terminates_transaction: false,
                transaction_req: Requirement::Optional,
                database_req: Requirement::ObligatoryToHave
            }
        }
    }
}

impl SelectStatement {
    pub fn take_where_expression(&mut self) -> Expression {
        self.where_expr.take().unwrap()
    }

    pub fn has_where_expression(&self) -> bool {
        self.where_expr.is_some()
    }

    pub fn is_sorted(&self) -> bool {
        self.sort.is_some()
    }

    pub fn is_limit(&self) -> bool {
        match self.limit {
            Limit::Some(_) => true,
            Limit::None => false,
        }
    }

    pub fn get_limit(&self) -> usize {
        match self.limit {
            Limit::Some(n) => n,
            Limit::None => panic!("Illegal code path"),
        }
    }
}