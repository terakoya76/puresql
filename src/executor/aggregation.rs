use column::column::Column;
use tuple::tuple::Tuple;
use field::field::Field;
use executor::table_scan::TableScanExec;
use executor::aggregator::Aggregator;

pub struct AggregationExec<'a, 't: 'a> {
    pub counter: usize,
    pub inputs: &'a mut TableScanExec<'t>,
    pub aggregators: Vec<Box<Aggregator>>,
}

impl<'a, 't: 'a> AggregationExec<'a, 't> {
    pub fn new(inputs: &'a mut TableScanExec<'t>, aggregators: Vec<Box<Aggregator>>) -> AggregationExec<'a, 't> {
        AggregationExec {
            counter: 0,
            inputs: inputs,
            aggregators: aggregators,
        }
    }

    pub fn increament(&mut self) {
        self.counter += 1;
    }
}

impl<'a, 't: 'a> Iterator for AggregationExec<'a, 't> {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        loop {
            match self.inputs.next() {
                None => return None,
                Some(tuple) => {
                    self.increament();
                    let mut fields: Vec<Field> = Vec::new();
                    for ref mut aggregator in &mut self.aggregators {
                        aggregator.update(&tuple, &self.inputs.columns);
                        fields.push(aggregator.fetch_result());
                    }
                    return Some(Tuple::new(fields));
                },
            }
        }
    }
}

