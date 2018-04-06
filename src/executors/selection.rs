use std::marker::PhantomData;

// trait
use ScanExec;
use Selector;

// struct
use columns::column::Column;
use tables::field::Field;
use tables::tuple::Tuple;

pub struct SelectionExec<'s, 't: 's, T: 't> {
    inputs: &'s mut T,
    selectors: Vec<Box<Selector>>,
    _marker: PhantomData<&'t T>,
}

impl<'s, 't, T> SelectionExec<'s, 't, T>
    where T: ScanExec {
    pub fn new(inputs: &'s mut T, selectors: Vec<Box<Selector>>) -> SelectionExec<'s, 't, T> {
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

    fn get_field(&mut self, handle: usize, column_name: &str) -> Field {
        self.inputs.get_field(handle, column_name)
    }

    fn set_next_handle(&mut self, next_handle: usize) {
    }

    fn next_handle(&mut self) -> Option<usize> {
        None
    }
}

// TODO: impl OR conditions
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

