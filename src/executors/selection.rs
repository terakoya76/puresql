use std::marker::PhantomData;

use ScanIterator;
use { Selectors, eval_selectors };
use meta::table_info::TableInfo;
use columns::column::Column;
use tables::tuple::Tuple;

pub struct SelectionExec<'s, 't: 's, T: 't> {
    inputs: &'s mut T,
    selectors: Option<Selectors>,
    _marker: PhantomData<&'t T>,
}

impl<'s, 't, T> SelectionExec<'s, 't, T>
    where T: ScanIterator {
    pub fn new(inputs: &'s mut T, selectors: Option<Selectors>) -> SelectionExec<'s, 't, T> {
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

impl<'s, 't, T> Iterator for SelectionExec<'s, 't, T>
    where T: ScanIterator {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        loop {
            match self.inputs.next() {
                None => return None,
                Some(tuple) => {
                    let passed: bool = match self.selectors.clone() {
                        None => true,
                        Some(selectors) => {
                            eval_selectors(
                                selectors,
                                &tuple,
                                &self.get_columns()
                            )
                        }
                    };

                    if passed {
                        return Some(tuple);
                    }
                },
            }
        }
    }
}
