use context::Context;
use meta::table_info::TableInfo;
use meta::column_info::ColumnInfo;
use columns::range::Range;
use tables::field::Field;
use allocators::allocator::Allocator;

use parser::statement::*;
use parser::parser::{Parser, ParseError};
use executors::memory_table_scan::MemoryTableScanExec;

#[derive(Debug)]
pub struct Client {
   pub ctx: Context,
}

impl Client {
    pub fn new(ctx: Context) -> Client {
        Client {
            ctx: ctx,
        }
    }

    pub fn handle_query(&mut self, query: &str) -> Result<(), ClientError> {
        let mut parser: Parser = Parser::new(query);
        let stmt = try!(parser.parse());
        match stmt.clone() {
            Statement::DDL(stmt) => exec_ddl(&mut self.ctx, stmt),
            Statement::DML(stmt) => exec_dml(&mut self.ctx, stmt),
        }
    }
}

pub fn exec_ddl(ctx: &mut Context, stmt: DDL) -> Result<(), ClientError> {
    match stmt {
        DDL::Create(stmt) => exec_create(ctx, stmt),
    }
}

pub fn exec_create(ctx: &mut Context, stmt: CreateStmt) -> Result<(), ClientError> {
    match stmt {
        CreateStmt::Table(stmt) => create_table_stmt(ctx, stmt),
    }
}

pub fn create_table_stmt(ctx: &mut Context, stmt: CreateTableStmt) -> Result<(), ClientError> {
    let columns: Vec<ColumnInfo> = stmt.columns.into_iter().enumerate().map(|(i, col)| ColumnInfo {
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
        None => return Err(ClientError::DatabaseNotFoundError),
        Some(ref mut db) => db.add_table(table_info.clone()),
    }

    ctx.table_id_alloc.increment();
    Ok(())
}

pub fn exec_dml(ctx: &mut Context, stmt: DML) -> Result<(), ClientError> {
    match stmt {
        DML::Insert(stmt) => exec_insert(ctx, stmt),
        DML::Select(stmt) => exec_select(ctx, stmt),
        _ => Err(ClientError::BuildExecutorError),
    }
}

pub fn exec_insert(ctx: &mut Context, stmt: InsertStmt) -> Result<(), ClientError> {
    let mut fields: Vec<Field> = Vec::new();
    let literals = stmt.values;
    for lit in literals {
        fields.push(Field::from_literal(lit));
    }

    match ctx.db {
        None => Err(ClientError::BuildExecutorError),
        Some(ref mut db) => {
            match db.load_table(&stmt.table_name) {
                Ok(ref mut mem_tbl) => Ok(mem_tbl.insert(fields)),
                _ => Err(ClientError::BuildExecutorError),
            }
        },
    }
}

pub fn exec_select(ctx: &mut Context, stmt: SelectStmt) -> Result<(), ClientError> {
    match ctx.db {
        None => Err(ClientError::BuildExecutorError),
        Some(ref mut db) => {
            let table_name: String = stmt.sources[0].clone();
            match db.load_table(&table_name) {
                Ok(ref mut mem_tbl) => {
                    match stmt.sources.len() {
                        1 => {
                            let mut scan_exec: MemoryTableScanExec = MemoryTableScanExec::new(mem_tbl, vec![Range::new(0, 10)]);
                            loop {
                                match scan_exec.next() {
                                    None => break,
                                    Some(tuple) => tuple.print(),
                                };
                            }
                            println!("Scaned\n");
                            Ok(())
                        },
                        _ => Err(ClientError::BuildExecutorError),
                    }
                },
                _ => Err(ClientError::BuildExecutorError),
            }
        },
    }
}

#[derive(Debug, PartialEq)]
pub enum ClientError {
    ParseError(ParseError),
    BuildExecutorError,
    DatabaseNotFoundError,
}

impl From<ParseError> for ClientError {
    fn from(err: ParseError) -> ClientError {
        ClientError::ParseError(err)
    }
}

