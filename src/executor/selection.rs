use column::column::Column;
use tuple::tuple::Tuple;
use executor::table_scan::TableScanExec;

pub struct SelectionExec<'s, 'ts: 's, 't: 'ts, 'm: 't> {
    inputs: &'s mut TableScanExec<'ts, 't, 'm>,
    selectors: Vec<Box<Fn(&Tuple, &Vec<Column>) -> bool>>,
}

impl<'s, 'ts, 't, 'm> SelectionExec<'s, 'ts, 't, 'm> {
    pub fn new(inputs: &'s mut TableScanExec<'ts, 't, 'm>, selectors: Vec<Box<Fn(&Tuple, &Vec<Column>) -> bool>>) -> SelectionExec<'s, 'ts, 't, 'm> {
        SelectionExec {
            inputs: inputs,
            selectors: selectors,
        }
    }
}

impl<'s, 'ts, 't, 'm> Iterator for SelectionExec<'s, 'ts, 't, 'm> {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        loop {
            match self.inputs.next() {
                None => return None,
                Some(tuple) => {
                    let mut passed: bool = true;
                    for ref selector in &self.selectors {
                        if !(selector)(&tuple, &self.inputs.columns) {
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

