use std::marker::PhantomData;

use ScanIterator;

use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;

#[derive(Debug)]
pub struct ProjectionExec<'p, 't: 'p, T: 't> {
    pub inputs: &'p mut T,
    pub projectors: Vec<String>,
    _marker: PhantomData<&'t T>,
}

impl<'p, 't, T> ProjectionExec<'p, 't, T>
    where T: ScanIterator {
    pub fn new(inputs: &'p mut T, projectors: Vec<String>) -> ProjectionExec<'p, 't, T> {
        ProjectionExec {
            inputs: inputs,
            projectors: projectors,
            _marker: PhantomData,
        }
    }
}

impl<'p, 't, T> ScanIterator for ProjectionExec<'p, 't, T>
    where T: ScanIterator {
    fn get_columns(&self) -> Vec<Column> {
        self.inputs.get_columns()
    }
}

impl<'p, 't, T> Iterator for ProjectionExec<'p, 't, T>
    where T: ScanIterator {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        loop {
            match self.inputs.next() {
                None => return None,
                Some(tuple) => {
                    let mut fields: Vec<Field> = Vec::new();
                    let designated_columns: Vec<String> = self.projectors.iter().map(|c| c.to_string()).collect();
                    for column in &self.inputs.get_columns() {
                        if designated_columns.contains(&column.name) {
                            fields.push(tuple.fields[column.offset].clone());
                        }
                    }
                    return Some(Tuple::new(fields));
                },
            }
        }
    }
}

