// TODO: impl Iterator
// it's necessary for pipe processing

use std::marker::PhantomData;

// trait
use executor::scan_exec::ScanExec;

// struct
use tuple::tuple::Tuple;

pub struct NestedLoopJoinExec<'n, 't: 'n, T1: 't, T2: 't> {
    cursor: usize,
    result_tuples: Vec<Tuple>,
    inner_table: &'n mut T1,
    outer_table: &'n mut T2,
    _marker: PhantomData<&'t T1>,
}

impl<'n, 't, T1, T2> NestedLoopJoinExec<'n, 't, T1, T2>
    where T1: ScanExec, T2: ScanExec {
    pub fn new(inner_table: &'n mut T1, outer_table: &'n mut T2) -> NestedLoopJoinExec<'n, 't, T1, T2> {
        NestedLoopJoinExec {
            cursor: 0,
            result_tuples: Vec::new(),
            inner_table: inner_table,
            outer_table: outer_table,
            _marker: PhantomData,
        }
    }

    pub fn join(&mut self) -> Vec<Tuple> {
        loop {
            match *&mut self.inner_table.next() {
                None => break,
                Some(ref inner_tuple) => {
                    loop {
                        match *&mut self.outer_table.next() {
                            None => break,
                            Some(ref outer_tuple) => {
                                let joined_tuple: Tuple = inner_tuple.append(outer_tuple);
                                &mut self.result_tuples.push(joined_tuple);
                            }
                        }
                    }
                }
            }
        }
        self.result_tuples.clone()
    }
}

