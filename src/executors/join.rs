use std::marker::PhantomData;

// trait
use ScanExec;

// struct
use columns::column::Column;
use tables::field::Field;
use tables::tuple::Tuple;

#[derive(Debug)]
pub struct NestedLoopJoinExec<'n, 't: 'n, T1: 't, T2: 't> {
    cursor: usize,
    inner_table: &'n mut T1,
    outer_table: &'n mut T2,
    _marker: PhantomData<&'t T1>,
}

impl<'n, 't, T1, T2> NestedLoopJoinExec<'n, 't, T1, T2>
    where T1: ScanExec, T2: ScanExec {
    pub fn new(inner_table: &'n mut T1, outer_table: &'n mut T2) -> NestedLoopJoinExec<'n, 't, T1, T2> {
        NestedLoopJoinExec {
            cursor: 0,
            inner_table: inner_table,
            outer_table: outer_table,
            _marker: PhantomData,
        }
    }
}

impl<'n, 't, T1, T2> ScanExec for NestedLoopJoinExec<'n, 't, T1, T2>
    where T1: ScanExec, T2: ScanExec {
    fn get_columns(&self) -> Vec<Column> {
        let mut inner_columns: Vec<Column> = self.inner_table.get_columns();
        let mut outer_columns: Vec<Column> = self.outer_table.get_columns();
        inner_columns.append(&mut outer_columns);
        inner_columns
    }

    fn get_tuple(&mut self, handle: usize) -> Tuple {
        Tuple::new(vec![])
    }

    fn set_next_handle(&mut self, next_handle: usize) {
    }

    fn next_handle(&mut self) -> Option<usize> {
        None
    }
}

impl<'n, 't, T1, T2> Iterator for NestedLoopJoinExec<'n, 't, T1, T2>
    where T1: ScanExec, T2: ScanExec {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        loop {
            match *&mut self.inner_table.next() {
                None => return None,
                Some(ref inner_tuple) => {
                    loop {
                        match *&mut self.outer_table.next() {
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
    }
}

