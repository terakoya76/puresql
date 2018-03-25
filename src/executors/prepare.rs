use meta::table_info::TableInfo;
use meta::column_info::ColumnInfo;

use allocators::allocator::Allocator;

use parser::statement::*;
use parser::parser::{Parser, ParseError};

#[derive(Debug)]
pub struct StmtExec {
    stmt: Statement,
}

#[derive(Debug)]
pub struct DDLExec {
    pub stmt: DDL,
}

#[derive(Debug)]
pub struct DMLExec {
    stmt: DML,
}

#[derive(Debug)]
pub struct PrepareExec {
   query: String,
}

impl PrepareExec {
    pub fn new(query: &str) -> PrepareExec {
        PrepareExec {
            query: query.to_owned(),
        }
    }

    pub fn exec(&self) -> Result<TableInfo, PrepareError> {
        let mut parser: Parser = Parser::new(&self.query);
        let stmt = try!(parser.parse());
        match stmt.clone() {
            Statement::DDL(stmt) => {
                match self.exec_ddl(stmt) {
                    None => Err(PrepareError::BuildExecutorError),
                    Some(table_info) => Ok(table_info),
                }
            },
            _ => Err(PrepareError::BuildExecutorError),
        }
    }

    pub fn exec_ddl(&self, stmt: DDL) -> Option<TableInfo> {
        match stmt {
            DDL::Create(stmt) => self.exec_create(stmt),
            _ => None,
        }
    }

    pub fn exec_create(&self, stmt: CreateStmt) -> Option<TableInfo> {
        match stmt {
            CreateStmt::Table(stmt) => {
                let table_info: TableInfo = self.create_table_stmt(stmt);
                Some(table_info)
            }
            _ => None,
        }
    }

    // TODO: impl global db context for holding meta info beyond lexical scope
    pub fn create_table_stmt(&self, stmt: CreateTableStmt) -> TableInfo {
        let columns = stmt.columns.into_iter().enumerate().map(|(i, col)| ColumnInfo {
            name: col.name,
            dtype: col.datatype,
            offset: i,
        }).collect();

        TableInfo {
            id: 1, // TODO: impl alloc uniq id
            name: stmt.table_name,
            columns: columns,
            indices: Vec::new(),
            next_record_id: Allocator::new(1), // TODO: need global var holding allocator uniquely
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum PrepareError {
    ParseError(ParseError),
    BuildExecutorError,
}

impl From<ParseError> for PrepareError {
    fn from(err: ParseError) -> PrepareError {
        PrepareError::ParseError(err)
    }
}

