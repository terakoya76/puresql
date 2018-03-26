use context::Context;
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

    pub fn exec(&self, ctx: &mut Context) -> Result<(), PrepareError> {
        let mut parser: Parser = Parser::new(&self.query);
        let stmt = try!(parser.parse());
        match stmt.clone() {
            Statement::DDL(stmt) => self.exec_ddl(ctx, stmt),
            Statement::DML(stmt) => self.exec_dml(ctx, stmt),
        }
    }

    pub fn exec_ddl(&self, ctx: &mut Context, stmt: DDL) -> Result<(), PrepareError> {
        match stmt {
            DDL::Create(stmt) => self.exec_create(ctx, stmt),
        }
    }

    pub fn exec_create(&self, ctx: &mut Context, stmt: CreateStmt) -> Result<(), PrepareError> {
        match stmt {
            CreateStmt::Table(stmt) => self.create_table_stmt(ctx, stmt),
        }
    }

    pub fn create_table_stmt(&self, ctx: &mut Context, stmt: CreateTableStmt) -> Result<(), PrepareError> {
        let columns = stmt.columns.into_iter().enumerate().map(|(i, col)| ColumnInfo {
            name: col.name,
            dtype: col.datatype,
            offset: i,
        }).collect();

        let table_info: TableInfo = TableInfo {
            id: ctx.table_id_alloc.base,
            name: stmt.table_name,
            columns: columns,
            indices: Vec::new(),
            next_record_id: Allocator::new(1),
        };

        match ctx.db {
            None => return Err(PrepareError::DatabaseNotFoundError),
            Some(ref mut db) => db.tables.push(table_info.clone()),
        }

        ctx.table_id_alloc.increment();
        Ok(())
    }

    pub fn exec_dml(&self, ctx: &mut Context, stmt: DML) -> Result<(), PrepareError> {
        match stmt {
            DML::Insert(stmt) => self.exec_insert(ctx, stmt),
            _ => Err(PrepareError::BuildExecutorError),
        }
    }

    pub fn exec_insert(&self, ctx: &mut Context, stmt: InsertStmt) -> Result<(), PrepareError> {
        println!("{:?}", stmt);
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub enum PrepareError {
    ParseError(ParseError),
    BuildExecutorError,
    DatabaseNotFoundError,
}

impl From<ParseError> for PrepareError {
    fn from(err: ParseError) -> PrepareError {
        PrepareError::ParseError(err)
    }
}

