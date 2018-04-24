use std::marker::PhantomData;

use ScanIterator;
use meta::table_info::TableInfo;
use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;
use parser::statement::*;

#[derive(Debug)]
pub struct ProjectionExec<'p, 't: 'p, T: 't> {
    pub inputs: &'p mut T,
    pub projectors: Vec<Projectable>,
    _marker: PhantomData<&'t T>,
}

impl<'p, 't, T> ProjectionExec<'p, 't, T>
    where T: ScanIterator {
    pub fn new(inputs: &'p mut T, projectors: Vec<Projectable>) -> ProjectionExec<'p, 't, T> {
        ProjectionExec {
            inputs: inputs,
            projectors: projectors,
            _marker: PhantomData,
        }
    }
}

impl<'p, 't, T> ScanIterator for ProjectionExec<'p, 't, T>
    where T: ScanIterator {
    fn get_meta(&self) -> TableInfo {
        self.inputs.get_meta()
    }

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
                    for target in &self.projectors {
                        match target {
                            &Projectable::Target(ref t) => {
                                for column in &self.inputs.get_columns() {
                                    let t: Target = t.clone();
                                    if t.table_name.is_some() {
                                        if t.table_name.unwrap() != column.table_name {
                                            continue;
                                        }
                                    }

                                    if t.name == column.name {
                                        fields.push(tuple.fields[column.offset].clone());
                                    }
                                }
                            },
                            &Projectable::Lit(ref l) => fields.push(l.clone().into()),
                            &Projectable::All => {
                                fields = tuple.fields.clone();
                            },
                            &Projectable::Aggregate(ref _a) => (),
                        }
                    }
                    return Some(Tuple::new(fields));
                },
            }
        }
    }
}
