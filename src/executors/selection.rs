use std::marker::PhantomData;

use ScanIterator;
use Selector;
use meta::table_info::TableInfo;
use columns::column::Column;
use tables::tuple::Tuple;

pub struct SelectionExec<'s, 't: 's, T: 't> {
    inputs: &'s mut T,
    selectors: Vec<Box<Selector>>,
    _marker: PhantomData<&'t T>,
}

impl<'s, 't, T> SelectionExec<'s, 't, T>
    where T: ScanIterator {
    pub fn new(inputs: &'s mut T, selectors: Vec<Box<Selector>>) -> SelectionExec<'s, 't, T> {
        SelectionExec {
            inputs: inputs,
            selectors: selectors,
            _marker: PhantomData,
        }
    }
}

impl<'s, 't, T> ScanIterator for SelectionExec<'s, 't, T>
    where T: ScanIterator {
    fn get_meta(&self) -> TableInfo {
        self.inputs.get_meta()
    }

    fn get_columns(&self) -> Vec<Column> {
        self.inputs.get_columns()
    }
}

// TODO: impl OR conditions
impl<'s, 't, T> Iterator for SelectionExec<'s, 't, T>
    where T: ScanIterator {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        loop {
            match self.inputs.next() {
                None => return None,
                Some(tuple) => {
                    let mut passed: bool = true;
                    for ref selector in &self.selectors {
                        if !selector.is_true(&tuple, &self.inputs.get_columns()) {
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

