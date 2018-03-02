use column::column::Column;
use tuple::tuple::Tuple;
use executor::table_scan::TableScanExec;

pub struct SelectionExec<'s, 't: 's> {
    inputs: &'s mut TableScanExec<'t>,
    selectors: Vec<Box<Fn(&Tuple, &Vec<Column>) -> bool>>,
}

impl<'s, 't: 's> SelectionExec<'s, 't> {
    pub fn new(inputs: &'s mut TableScanExec<'t>, selectors: Vec<Box<Fn(&Tuple, &Vec<Column>) -> bool>>) -> SelectionExec<'s, 't> {
        SelectionExec {
            inputs: inputs,
            selectors: selectors,
        }
    }
}

impl<'s, 't: 's> Iterator for SelectionExec<'s, 't> {
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

