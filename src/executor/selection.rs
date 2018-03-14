use std::marker::PhantomData;

// trait
use executor::scan_exec::ScanExec;

// struct
use column::column::Column;
use tuple::tuple::Tuple;
use executor::memory_table_scan::MemoryTableScanExec;

pub struct SelectionExec<'s, 't: 's, T: 't> {
    inputs: &'s mut T,
    selectors: Vec<Box<Fn(&Tuple, &Vec<Column>) -> bool>>,
    _marker: PhantomData<&'t T>,
}

impl<'s, 't, T> SelectionExec<'s, 't, T>
    where T: ScanExec {
    pub fn new(inputs: &'s mut T, selectors: Vec<Box<Fn(&Tuple, &Vec<Column>) -> bool>>) -> SelectionExec<'s, 't, T> {
        SelectionExec {
            inputs: inputs,
            selectors: selectors,
            _marker: PhantomData,
        }
    }
}

impl<'s, 't, T> Iterator for SelectionExec<'s, 't, T>
    where T: ScanExec {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        loop {
            match self.inputs.next() {
                None => return None,
                Some(tuple) => {
                    let mut passed: bool = true;
                    for ref selector in &self.selectors {
                        if !(selector)(&tuple, &self.inputs.get_columns()) {
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

