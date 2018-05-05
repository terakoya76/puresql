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
    pub source: DataSource,
    pub condition: Option<Conditions>,
    pub group_by: Option<Vec<Target>>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<isize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Projectable {
    Lit(Literal),
    Target(Target),
    Aggregate(Aggregate),
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub table_name: Option<String>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregate {
    Count(Aggregatable),
    Sum(Aggregatable),
    Average(Aggregatable),
    Max(Aggregatable),
    Min(Aggregatable),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregatable {
    Target(Target),
    All,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataSource {
    Leaf(Source),
    Join(Box<DataSource>, Box<DataSource>, Option<Conditions>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Source {
    Table(Table),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubQuery {}

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
    pub right: Comparable,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Comparable {
    Lit(Literal),
    Target(Target),
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
pub struct GroupBy {}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {}

#[derive(Debug, Clone, PartialEq)]
pub struct Limit {}

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

#[cfg(test)]
mod tests {}
