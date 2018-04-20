use ScanIterator;
use Selector;
use meta::table_info::{TableInfo, TableInfoError};
use meta::column_info::ColumnInfo;
use columns::column::Column;
use tables::tuple::Tuple;
use allocators::allocator::Allocator;

use parser::statement::*;
use executors::selector::*;

//#[derive(Debug)]
pub struct NestedLoopJoinExec<'n> {
    pub cursor: usize,
    pub outer_columns: Vec<Column>,
    pub inner_columns: Vec<Column>,
    pub next_tuple: Box<FnMut() -> Option<Tuple> + 'n>,
    pub selectors: Vec<Box<Selector>>,
    pub meta: TableInfo,
}

impl<'n> NestedLoopJoinExec<'n> {
    pub fn new<T1: ScanIterator, T2: ScanIterator>(outer_table: &'n mut T1, inner_table: &'n mut T2, condition: Option<Conditions>) -> NestedLoopJoinExec<'n> {
        let outer_column_length: usize = outer_table.get_meta().columns.len();
        let mut column_infos: Vec<ColumnInfo> = outer_table.get_meta().columns;
        for (i, column) in inner_table.get_meta().columns.iter().enumerate() {
            let col: ColumnInfo = column.clone();
            column_infos.push(ColumnInfo {
                name: col.name,
                dtype: col.dtype,
                offset: outer_column_length + i,
            });
        }
        let meta: TableInfo = TableInfo {
            id: 0,
            name: "".to_owned(),
            columns: column_infos,
            indices: Vec::new(),
            next_record_id: Allocator::new(1),
        };

        let selectors: Vec<Box<Selector>> = match condition {
            None => Vec::new(),
            Some(c) => execute_where(c, false),
        };

        NestedLoopJoinExec {
            cursor: 0,
            outer_columns: outer_table.get_columns(),
            inner_columns: inner_table.get_columns(),
            next_tuple: next_tuple(outer_table, inner_table),
            selectors: selectors,
            meta: meta,
        }
    }
}

impl<'n> ScanIterator for NestedLoopJoinExec<'n> {
    fn get_meta(&self) -> TableInfo {
        self.meta.clone()
    }

    fn get_columns(&self) -> Vec<Column> {
        let outer_length: usize = self.outer_columns.len();
        let mut outer_columns = self.outer_columns.clone();
        let mut inner_columns: Vec<Column> = self.inner_columns.clone().into_iter().map(|c| Column {
            table_name: c.table_name,
            name: c.name,
            dtype: c.dtype,
            offset: c.offset + outer_length,
        }).collect();

        outer_columns.append(&mut inner_columns);
        outer_columns
    }
}

impl<'n> Iterator for NestedLoopJoinExec<'n> {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        loop {
            match (self.next_tuple)() {
                None => return None,
                Some(tuple) => {
                    let mut passed: bool = true;
                    for ref selector in &self.selectors {
                        if !selector.is_true(&tuple, &self.get_columns()) {
                          passed = false;
                          break;
                        }
                    }

                    if passed {
                        return Some(tuple);
                    }
                },
            }
        }
    }
}

fn next_tuple<'n, T1: ScanIterator + 'n, T2: ScanIterator + 'n>(outer_table: &'n mut T1, inner_table: &'n mut T2) -> Box<FnMut() -> Option<Tuple> + 'n> {
    Box::new(move || {
        loop {
            match outer_table.next() {
                None => return None,
                Some(ref outer_tuple) => {
                    loop {
                        match inner_table.next() {
                            None => break,
                            Some(ref inner_tuple) => {
                                let joined_tuple: Tuple = outer_tuple.append(inner_tuple);
                                return Some(joined_tuple);
                            }
                        }
                    }
                }
            }
        }
    })
}

fn execute_where(condition: Conditions, is_or: bool) -> Vec<Box<Selector>> {
    match condition {
        Conditions::And(c1, c2) => {
            let mut selectors1: Vec<Box<Selector>> = execute_where(*c1, false);
            let mut selectors2: Vec<Box<Selector>> = execute_where(*c2, false);
            selectors1.append(&mut selectors2);
            selectors1
        },

        Conditions::Or(c1, c2) => {
            let mut selectors1: Vec<Box<Selector>> = execute_where(*c1, true);
            let mut selectors2: Vec<Box<Selector>> = execute_where(*c2, true);
            selectors1.append(&mut selectors2);
            selectors1
        },

        Conditions::Leaf(condition) => {
            match condition.op {
                Operator::Equ => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => vec![NotEqual::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![NotEqual::new(condition.left, Some(t), None)],
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => vec![Equal::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![Equal::new(condition.left, Some(t), None)],
                        }
                    }
                },

                Operator::NEqu => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => vec![Equal::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![Equal::new(condition.left, Some(t), None)],
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => vec![NotEqual::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![NotEqual::new(condition.left, Some(t), None)],
                        }
                    }
                },

                Operator::GT => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => vec![LE::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![LE::new(condition.left, Some(t), None)],
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => vec![GT::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![GT::new(condition.left, Some(t), None)],
                        }
                    }
                },

                Operator::LT => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => vec![GE::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![GE::new(condition.left, Some(t), None)],
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => vec![LT::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![LT::new(condition.left, Some(t), None)],
                        }
                    }
                },

                Operator::GE => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => vec![LT::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![LT::new(condition.left, Some(t), None)],
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => vec![GE::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![GE::new(condition.left, Some(t), None)],
                        }
                    }
                },

                Operator::LE => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => vec![GT::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![GT::new(condition.left, Some(t), None)],
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => vec![LE::new(condition.left, None, Some(l.into()))],
                            Comparable::Target(t) => vec![LE::new(condition.left, Some(t), None)],
                        }
                    }
                },
            }
        },
    }
}

#[derive(Debug, PartialEq)]
pub enum JoinExecError {
    TableInfoError(TableInfoError),
}

impl From<TableInfoError> for JoinExecError {
    fn from(err: TableInfoError) -> JoinExecError {
        JoinExecError::TableInfoError(err)
    }
}
