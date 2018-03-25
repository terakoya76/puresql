use data_type::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    DDL(DDL),
    DML(DML),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DDL {
    Create(CreateStmt),
    //Drop(DropStmt),
    //Alter(AlterStmt),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DML {
    //Use(UseStmt),
    //Describe(String),
    Select(SelectStmt),
    Update(UpdateStmt),
    Insert(InsertStmt),
    Delete(DeleteStmt),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CreateStmt {
    Table(CreateTableStmt),
    //View(ViewInfo),
    //Database(DatabaseInfo),
}

#[derive(Debug, Clone, PartialEq)]
pub enum DropStmt {
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterStmt {
}

#[derive(Debug, Clone, PartialEq)]
pub enum UseStmt {
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectStmt {
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateStmt {
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertStmt {
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeleteStmt {
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub datatype: DataType,
}

