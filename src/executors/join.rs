use std::marker::PhantomData;

use ScanExec;
use columns::column::Column;
use tables::tuple::Tuple;

//#[derive(Debug)]
pub struct NestedLoopJoinExec<'n> {
    cursor: usize,
    //inner_table: &'n mut T1,
    //outer_table: &'n mut T2,
    inner_columns: Vec<Column>,
    outer_columns: Vec<Column>,
    next_tuple: Box<FnMut() -> Option<Tuple> + 'n>,
    //_marker1: PhantomData<&'n T1>,
    //_marker2: PhantomData<&'t T2>,
}

impl<'n> NestedLoopJoinExec<'n> {
    pub fn new<T1: ScanExec, T2: ScanExec>(inner_table: &'n mut T1, outer_table: &'n mut T2) -> NestedLoopJoinExec<'n> {

        NestedLoopJoinExec {
            cursor: 0,
            //inner_table: inner_table,
            //outer_table: outer_table,
            inner_columns: inner_table.get_columns(),
            outer_columns: outer_table.get_columns(),
            next_tuple: next_tuple(inner_table, outer_table),
            //_marker1: PhantomData,
            //_marker2: PhantomData,
        }
    }
}

impl<'n> ScanExec for NestedLoopJoinExec<'n> {
    fn get_columns(&self) -> Vec<Column> {
        let mut inner_columns = self.inner_columns.clone();
        inner_columns.append(&mut self.outer_columns.clone());
        inner_columns
    }

    fn get_tuple(&mut self, _handle: usize) -> Tuple {
        Tuple::new(vec![])
    }

    fn set_next_handle(&mut self, _next_handle: usize) {
    }

    fn next_handle(&mut self) -> Option<usize> {
        None
    }
}

impl<'n> Iterator for NestedLoopJoinExec<'n> {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        (self.next_tuple)()
    }
}

fn next_tuple<'n, T1: ScanExec + 'n, T2: ScanExec + 'n>(inner_table: &'n mut T1, outer_table: &'n mut T2) -> Box<FnMut() -> Option<Tuple> + 'n> {
    Box::new(move || {
        loop {
            match inner_table.next() {
                None => return None,
                Some(ref inner_tuple) => {
                    loop {
                        match outer_table.next() {
                            None => break,
                            Some(ref outer_tuple) => {
                                let joined_tuple: Tuple = inner_tuple.append(outer_tuple);
                                return Some(joined_tuple);
                            }
                        }
                    }
                }
            }
        }
    })
}

