use crate::selection::Selection;
use crate::sql::expression::Expression;
use crate::value::{Type, Value};

pub enum Statement {
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Insert(InsertStatement),
    CreateTable(CreateTableStatement),
    CreateDatabase(String),
    Describe(String),
    StartTransaction,
    Rollback,
    Commit,
    ShowDatabases,
    ShowTables,
}

pub enum Limit {
    None,
    Some(usize)
}

pub struct SelectStatement {
    pub(crate) where_expr: Option<Expression>,
    pub(crate) selection: Selection,
    pub(crate) table_name: String,
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

pub struct CreateTableStatement {
    pub(crate) table_name: String,
    //Column name, Column type, is primary
    pub(crate) columns: Vec<(String, Type, bool)>
}

enum Requiremnt {
    ObligatoryToNotHave,
    ObligatoryToHave,
    Optional,
}

struct StatementRequirement {
    transaction: Requiremnt,
    database: Requiremnt,
}

impl Statement {
    pub fn terminates_transaction(&self) -> bool {
        match *self {
            Statement::Rollback | Statement::Commit => true,
            _ => false
        }
    }

    pub fn requires_transaction(&self) -> bool {
        match self.get_requirements().transaction {
            Requiremnt::ObligatoryToHave => true,
            Requiremnt::ObligatoryToNotHave => false,
            Requiremnt::Optional => false,
        }
    }

    pub fn requires_database(&self) -> bool {
        match self.get_requirements().database {
            Requiremnt::ObligatoryToHave => true,
            Requiremnt::ObligatoryToNotHave => false,
            Requiremnt::Optional => false,
        }
    }

    fn get_requirements(&self) -> StatementRequirement {
        match self {
            Statement::Select(_) => StatementRequirement { transaction: Requiremnt::ObligatoryToHave, database: Requiremnt::ObligatoryToHave },
            Statement::Update(_) => StatementRequirement { transaction: Requiremnt::ObligatoryToHave, database: Requiremnt::ObligatoryToHave },
            Statement::Delete(_) => StatementRequirement { transaction: Requiremnt::ObligatoryToHave, database: Requiremnt::ObligatoryToHave },
            Statement::Insert(_) => StatementRequirement { transaction: Requiremnt::ObligatoryToHave, database: Requiremnt::ObligatoryToHave },
            Statement::CreateTable(_) => StatementRequirement { transaction: Requiremnt::Optional, database: Requiremnt::ObligatoryToHave },
            Statement::CreateDatabase(_) => StatementRequirement { transaction: Requiremnt::Optional, database: Requiremnt::ObligatoryToNotHave },
            Statement::Describe(_) => StatementRequirement { transaction: Requiremnt::Optional, database: Requiremnt::ObligatoryToHave },
            Statement::StartTransaction => StatementRequirement { transaction: Requiremnt::ObligatoryToNotHave, database: Requiremnt::ObligatoryToHave },
            Statement::Rollback => StatementRequirement { transaction: Requiremnt::ObligatoryToHave, database: Requiremnt::ObligatoryToHave },
            Statement::Commit => StatementRequirement { transaction: Requiremnt::ObligatoryToHave, database: Requiremnt::ObligatoryToHave },
            Statement::ShowDatabases => StatementRequirement { transaction: Requiremnt::Optional, database: Requiremnt::ObligatoryToNotHave },
            Statement::ShowTables => StatementRequirement { transaction: Requiremnt::Optional, database: Requiremnt::ObligatoryToHave },
        }
    }
}

impl UpdateStatement {
    pub fn get_updated_values(&self) -> Selection {
        let mut column_names = Vec::new();
        for (column_name, _) in &self.updated_values {
            column_names.push(column_name.clone());
        }

        Selection::Some(column_names)
    }
}