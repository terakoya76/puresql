use std::marker::PhantomData;

// trait
use ScanExec;

// struct
use columns::column::Column;
use tables::tuple::Tuple;

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

impl<'s, 't, T> ScanExec for SelectionExec<'s, 't, T>
    where T: ScanExec {
    fn get_columns(&self) -> Vec<Column> {
        self.inputs.get_columns()
    }

    fn get_tuple(&mut self, handle: usize) -> Tuple {
        self.inputs.get_tuple(handle)
    }

    fn set_next_handle(&mut self, next_handle: usize) {
    }

    fn next_handle(&mut self) -> Option<usize> {
        None
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

