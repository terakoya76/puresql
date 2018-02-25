use tuple::tuple::Tuple;
use executor::table_scan::TableScanExec;

pub struct NestedLoopJoinExec<'a> {
    cursor: usize,
    result_tuples: Vec<Tuple>,
    inner_table: TableScanExec<'a>,
    outer_table: TableScanExec<'a>,
}

impl<'a> NestedLoopJoinExec<'a> {
    pub fn new(inner_table: TableScanExec<'a>, outer_table: TableScanExec<'a>) -> NestedLoopJoinExec<'a> {
        NestedLoopJoinExec {
            cursor: 0,
            result_tuples: Vec::new(),
            inner_table: inner_table,
            outer_table: outer_table,
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

