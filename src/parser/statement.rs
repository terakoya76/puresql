use data_type::DataType;
use parser::token::Literal;

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
pub struct SelectStmt {
    pub targets: Vec<Projectable>,
    pub source: DataSrc,
    pub condition: Option<Conditions>,
    pub group_by: Option<GroupBy>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<Limit>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    pub table_name: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DataSrc {
    pub tables: Vec<String>,
    pub condition: Option<Conditions>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubQuery {
}

#[derive(Debug, Clone, PartialEq)]
pub enum Conditions {
    Leaf(Condition),
    And(Box<Conditions>, Box<Conditions>),
    Or(Box<Conditions>, Box<Conditions>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub left: Target,
    pub op: Operator,
    pub right: Projectable,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operator {
    Equ,
    NEqu,
    GT,
    LT,
    GE,
    LE,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Projectable {
    Lit(Literal),
    Target(Target),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GroupBy {
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
}

#[derive(Debug, Clone, PartialEq)]
pub struct Limit {
}

#[derive(Debug, Clone, PartialEq)]
pub enum UpdateStmt {
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub values: Vec<Literal>,
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

