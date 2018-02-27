use column::column::Column;
use tuple::tuple::Tuple;
use executor::table_scan::TableScanExec;

pub struct SelectionExec<'a> {
    inputs: &'a mut TableScanExec<'a>,
    filters: Vec<Box<Fn(&Tuple, &Vec<Column>) -> bool>>,
}

impl<'a> SelectionExec<'a> {
    pub fn new(inputs: &'a mut TableScanExec<'a>, filters: Vec<Box<Fn(&Tuple, &Vec<Column>) -> bool>>) -> SelectionExec<'a> {
        SelectionExec {
            inputs: inputs,
            filters: filters,
        }
    }
}

impl<'a> Iterator for SelectionExec<'a> {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        loop {
            match self.inputs.next() {
                None => return None,
                Some(tuple) => {
                    let mut passed: bool = true;
                    for ref filter in &self.filters {
                        if !(filter)(&tuple, &self.inputs.columns) {
                          passed = false;
                          break;
                        }
                    }

                    if passed {
                        return Some(tuple);
                    }
                }
            }
        }
    }
}

