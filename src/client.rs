use context::Context;
use database::DatabaseError;
use meta::table_info::{TableInfo, TableInfoError};
use meta::column_info::ColumnInfo;
use columns::range::Range;
use tables::memory_table::MemoryTable;
use tables::field::Field;
use allocators::allocator::Allocator;

use parser::statement::*;
use parser::parser::{Parser, ParseError};
use executors::memory_table_scan::MemoryTableScanExec;
use executors::projection::ProjectionExec;
use executors::selection::SelectionExec;
use executors::selector::*;
use executors::join::NestedLoopJoinExec;

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
        fields.push(lit.into());
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
    println!("{:?}", stmt);
    match ctx.db {
        None => Err(ClientError::BuildExecutorError),
        Some(ref mut db) => {
            match stmt.sources.len() {
                1 => {
                    let table_name: String = stmt.sources[0].clone();
                    match db.clone().load_table(&table_name) {
                        Ok(ref mut mem_tbl) => {
                            let mut scan_exec: MemoryTableScanExec = MemoryTableScanExec::new(mem_tbl, vec![Range::new(0, 10)]);

                            let mut conditions: Vec<Box<Selector>> = Vec::new();
                            match stmt.condition {
                                None => {},
                                Some(condition) => {
                                    let tbl_info: TableInfo = try!(db.table_info_from_str(&table_name));
                                    let column_info: ColumnInfo = try!(tbl_info.column_info_from_str(&condition.column));
                                    match condition.op {
                                        Equ => {
                                            let right_side = match condition.right_side {
                                                Comparable::Lit(l) => Equal::new(&condition.column, None, Some(l.into())),
                                                Comparable::Word(s) => return Err(ClientError::BuildExecutorError),
                                            };
                                            conditions.push(right_side);
                                        },
                                        NEqu => {},
                                        GT => {},
                                        LT => {},
                                        GE => {},
                                        LE => {},
                                    }
                                },
                            }

                            let mut selection_exec: SelectionExec<MemoryTableScanExec> = SelectionExec::new(&mut scan_exec, conditions);
                            let mut proj_exec: ProjectionExec<SelectionExec<MemoryTableScanExec>> = ProjectionExec::new(&mut selection_exec, stmt.targets);

                            loop {
                                match proj_exec.next() {
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
                2 => {
                    let mut db4left = db.clone();
                    let left_tbl_name: String = stmt.sources[0].clone();
                    let mut left_tbl: MemoryTable = match db4left.load_table(&left_tbl_name) {
                        Ok(mem_tbl) => mem_tbl,
                        _ => return Err(ClientError::BuildExecutorError),
                    };
                    let mut left_tbl_scan: MemoryTableScanExec = MemoryTableScanExec::new(&mut left_tbl, vec![Range::new(0, 10)]);

                    let mut db4rht = db.clone();
                    let rht_tbl_name: String = stmt.sources[1].clone();
                    let mut rht_tbl: MemoryTable = match db4rht.load_table(&rht_tbl_name) {
                        Ok(mem_tbl) => mem_tbl,
                        _ => return Err(ClientError::BuildExecutorError),
                    };
                    let mut rht_tbl_scan: MemoryTableScanExec = MemoryTableScanExec::new(&mut rht_tbl, vec![Range::new(0, 10)]);

                    let mut join_exec: NestedLoopJoinExec<MemoryTableScanExec, MemoryTableScanExec> = NestedLoopJoinExec::new(&mut left_tbl_scan, &mut rht_tbl_scan);
                    let mut proj_exec: ProjectionExec<NestedLoopJoinExec<MemoryTableScanExec, MemoryTableScanExec>> = ProjectionExec::new(&mut join_exec, stmt.targets);

                    loop {
                        match proj_exec.next() {
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
    }
}

#[derive(Debug, PartialEq)]
pub enum ClientError {
    ParseError(ParseError),
    DatabaseError(DatabaseError),
    TableInfoError(TableInfoError),
    BuildExecutorError,
    DatabaseNotFoundError,
}

impl From<ParseError> for ClientError {
    fn from(err: ParseError) -> ClientError {
        ClientError::ParseError(err)
    }
}

impl From<DatabaseError> for ClientError {
    fn from(err: DatabaseError) -> ClientError {
        ClientError::DatabaseError(err)
    }
}

impl From<TableInfoError> for ClientError {
    fn from(err: TableInfoError) -> ClientError {
        ClientError::TableInfoError(err)
    }
}

