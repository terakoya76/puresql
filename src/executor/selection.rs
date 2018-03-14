use column::column::Column;
use tuple::tuple::Tuple;
use executor::memory_table_scan::MemoryTableScanExec;

pub struct SelectionExec<'s, 'ts: 's, 't: 'ts> {
    inputs: &'s mut MemoryTableScanExec<'ts, 't>,
    selectors: Vec<Box<Fn(&Tuple, &Vec<Column>) -> bool>>,
}

impl<'s, 'ts, 't> SelectionExec<'s, 'ts, 't> {
    pub fn new(inputs: &'s mut MemoryTableScanExec<'ts, 't>, selectors: Vec<Box<Fn(&Tuple, &Vec<Column>) -> bool>>) -> SelectionExec<'s, 'ts, 't> {
        SelectionExec {
            inputs: inputs,
            selectors: selectors,
        }
    }
}

impl<'s, 'ts, 't> Iterator for SelectionExec<'s, 'ts, 't> {
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

