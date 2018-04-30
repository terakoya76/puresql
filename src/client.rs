use context::Context;
use database::{Database, DatabaseError};
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
use executors::aggregation::AggregationExec;
use executors::aggregator::*;

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
    };

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
            match db.load_table(stmt.table_name) {
                Ok(ref mut mem_tbl) => {
                    mem_tbl.insert(fields);
                    Ok(())
                },
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
            let mut conditions: Vec<Box<Selector>> = Vec::new();
            match stmt.condition.clone() {
                None => {},
                Some(condition) => {
                    conditions = try!(build_selectors(condition, false));
                },
            }

            match stmt.source.clone() {
                DataSource::Join(_s1, _s2, _c) => {
                    let mut scan_exec: NestedLoopJoinExec = try!(exec_join(db.clone(), stmt.source));
                    let mut selection_exec = SelectionExec::new(&mut scan_exec, conditions);

                    let mut aggregators: Vec<Box<Aggregator>> = Vec::new();
                    for target in stmt.targets.clone() {
                        match target {
                            Projectable::Aggregate(expr) => aggregators.push(try!(build_aggregator(expr))),
                            _ => (),
                        }
                    }

                    if aggregators.len() > 0 {
                        let group_keys = match stmt.group_by {
                            None => vec![],
                            Some(v) => v,
                        };

                        let mut aggr_exec = AggregationExec::new(&mut selection_exec, group_keys, aggregators);
                        loop {
                            match aggr_exec.next() {
                                None => break,
                                Some(tuples) => {
                                    for tuple in tuples {
                                        tuple.print();
                                    }
                                },
                            };
                        }
                    } else {
                        let mut proj_exec = ProjectionExec::new(&mut selection_exec, stmt.targets);
                        loop {
                            match proj_exec.next() {
                                None => break,
                                Some(tuple) => tuple.print(),
                            };
                        }
                    }
                    println!("Scaned\n");
                },
                DataSource::Leaf(_s) => {
                    let mut scan_exec: MemoryTableScanExec = try!(exec_scan(db.clone(), stmt.source));
                    let mut selection_exec = SelectionExec::new(&mut scan_exec, conditions);
                    
                    let mut aggregators: Vec<Box<Aggregator>> = Vec::new();
                    for target in stmt.targets.clone() {
                        match target {
                            Projectable::Aggregate(expr) => aggregators.push(try!(build_aggregator(expr))),
                            _ => (),
                        }
                    }

                    if aggregators.len() > 0 {
                        let group_keys = match stmt.group_by {
                            None => vec![],
                            Some(v) => v,
                        };

                        let mut aggr_exec = AggregationExec::new(&mut selection_exec, group_keys, aggregators);
                        loop {
                            match aggr_exec.next() {
                                None => break,
                                Some(tuples) => {
                                    for tuple in tuples {
                                        tuple.print();
                                    }
                                },
                            };
                        }
                    } else {
                        let mut proj_exec = ProjectionExec::new(&mut selection_exec, stmt.targets);
                        loop {
                            match proj_exec.next() {
                                None => break,
                                Some(tuple) => tuple.print(),
                            };
                        }
                    }
                    println!("Scaned\n");
                },
            }
            Ok(())
        }
    }
}

pub fn exec_join<'i>(db: Database, source: DataSource) -> Result<NestedLoopJoinExec<'i>, ClientError> {
    match source {
        DataSource::Join(s1, s2, c) => {
            match *s1.clone() {
                DataSource::Join(_s1, _s2, _c) => {
                    let iter1 = try!(exec_join(db.clone(), *s1));
                    match *s2.clone() {
                        DataSource::Join(_s1, _s2, _c) => {
                            let iter2 = try!(exec_join(db.clone(), *s2));
                            Ok(NestedLoopJoinExec::new(iter1, iter2, c))
                        },
                        DataSource::Leaf(_s) => {
                            let iter2 = try!(exec_scan(db.clone(), *s2));
                            Ok(NestedLoopJoinExec::new(iter1, iter2, c))
                        },
                    }
                },
                DataSource::Leaf(_s) => {
                    let iter1 = try!(exec_scan(db.clone(), *s1));
                    match *s2.clone() {
                        DataSource::Join(_s1, _s2, _c) => {
                            let iter2 = try!(exec_join(db.clone(), *s2));
                            Ok(NestedLoopJoinExec::new(iter1, iter2, c))
                        },
                        DataSource::Leaf(_s) => {
                            let iter2 = try!(exec_scan(db.clone(), *s2));
                            Ok(NestedLoopJoinExec::new(iter1, iter2, c))
                        },
                    }
                },
            }
        },
        _ => Err(ClientError::BuildExecutorError),
    }
}

pub fn exec_scan(mut db: Database, source: DataSource) -> Result<MemoryTableScanExec, ClientError> {
    match source {
        DataSource::Leaf(s) => {
            match s {
                Source::Table(t) => {
                    let tbl_name: String = t.name.clone();
                    let mut mem_tbl: &MemoryTable = try!(db.load_table(tbl_name));
                    let mem_tbl_info: TableInfo = mem_tbl.meta.clone();
                    Ok(MemoryTableScanExec::new(mem_tbl.clone(), mem_tbl_info, vec![Range::new(0, 10)]))
                },
            }
        },
        _ => Err(ClientError::BuildExecutorError),
    }
}

#[derive(Debug, PartialEq)]
pub enum ClientError {
    ParseError(ParseError),
    DatabaseError(DatabaseError),
    TableInfoError(TableInfoError),
    SelectorError(SelectorError),
    AggregatorError(AggregatorError),
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

impl From<SelectorError> for ClientError {
    fn from(err: SelectorError) -> ClientError {
        ClientError::SelectorError(err)
    }
}

impl From<AggregatorError> for ClientError {
    fn from(err: AggregatorError) -> ClientError {
        ClientError::AggregatorError(err)
    }
}
