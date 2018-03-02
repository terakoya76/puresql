use column::column::Column;
use tuple::tuple::Tuple;
use field::field::Field;
use executor::table_scan::TableScanExec;

pub struct ProjectionExec<'p, 't: 'p> {
    inputs: &'p mut TableScanExec<'t>,
    projectors: Vec<&'p str>,
}

impl<'p, 't: 'p> ProjectionExec<'p, 't> {
    pub fn new(inputs: &'p mut TableScanExec<'t>, projectors: Vec<&'p str>) -> ProjectionExec<'p, 't> {
        ProjectionExec {
            inputs: inputs,
            projectors: projectors,
        }
    }
}

impl<'p, 't: 'p> Iterator for ProjectionExec<'p, 't> {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        loop {
            match self.inputs.next() {
                None => return None,
                Some(tuple) => {
                    let mut fields: Vec<Field> = Vec::new();
                    let designated_columns: Vec<String> = self.projectors.iter().map(|c| c.to_string()).collect();
                    for column in &self.inputs.columns {
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

