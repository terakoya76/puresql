use column::column::Column;
use tuple::tuple::Tuple;
use field::field::Field;
use executor::table_scan::TableScanExec;

pub struct ProjectionExec<'p, 'ts: 'p, 't: 'ts, 'm: 't> {
    inputs: &'p mut TableScanExec<'ts, 't, 'm>,
    projectors: Vec<&'p str>,
}

impl<'p, 'ts, 't, 'm> ProjectionExec<'p, 'ts, 't, 'm> {
    pub fn new(inputs: &'p mut TableScanExec<'ts, 't, 'm>, projectors: Vec<&'p str>) -> ProjectionExec<'p, 'ts, 't, 'm> {
        ProjectionExec {
            inputs: inputs,
            projectors: projectors,
        }
    }
}

impl<'p, 'ts, 't, 'm> Iterator for ProjectionExec<'p, 'ts, 't, 'm> {
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

