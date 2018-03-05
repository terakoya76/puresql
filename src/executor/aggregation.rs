use column::column::Column;
use tuple::tuple::Tuple;
use field::field::Field;
use executor::table_scan::TableScanExec;
use executor::aggregator::Aggregator;

pub struct AggregationExec<'a, 't: 'a> {
    pub counter: usize,
    pub inputs: &'a mut TableScanExec<'t>,
    pub aggregators: Vec<Aggregator>,
}

impl<'a, 't: 'a> AggregationExec<'a, 't> {
    pub fn new(inputs: &'a mut TableScanExec<'t>, aggregators: Vec<Aggregator>) -> AggregationExec<'a, 't> {
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
                        let next_value: Field = (aggregator.function)(self.counter, &aggregator.result, &tuple, &self.inputs.columns);
                        aggregator.update(next_value);
                        fields.push(aggregator.result.clone());
                    }
                    return Some(Tuple::new(fields));
                },
            }
        }
    }
}

