use tuple::tuple::Tuple;
use field::field::Field;
use executor::memory_table_scan::MemoryTableScanExec;

pub struct ProjectionExec<'p, 'ts: 'p, 't: 'ts> {
    inputs: &'p mut MemoryTableScanExec<'ts, 't>,
    projectors: Vec<&'p str>,
}

impl<'p, 'ts, 't> ProjectionExec<'p, 'ts, 't> {
    pub fn new(inputs: &'p mut MemoryTableScanExec<'ts, 't>, projectors: Vec<&'p str>) -> ProjectionExec<'p, 'ts, 't> {
        ProjectionExec {
            inputs: inputs,
            projectors: projectors,
        }
    }
}

impl<'p, 'ts, 't> Iterator for ProjectionExec<'p, 'ts, 't> {
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

