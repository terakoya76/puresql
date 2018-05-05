use std::marker::PhantomData;
use std::collections::HashMap;

use ScanIterator;
use Aggregator;
use tables::tuple::Tuple;
use tables::field::Field;

use parser::statement::*;

pub struct AggregationExec<'a, 't: 'a, T: 't> {
    pub group_keys: Vec<Target>,
    pub inputs: &'a mut T,
    pub aggregators: Vec<Aggregator>,
    pub grouped_aggregators: HashMap<Vec<Field>, Vec<Aggregator>>,
    _marker: PhantomData<&'t T>,
}

impl<'a, 't, T> AggregationExec<'a, 't, T>
where
    T: ScanIterator,
{
    pub fn new(
        inputs: &'a mut T,
        group_keys: Vec<Target>,
        aggregators: Vec<Aggregator>,
    ) -> AggregationExec<'a, 't, T> {
        AggregationExec {
            group_keys: group_keys,
            inputs: inputs,
            aggregators: aggregators,
            grouped_aggregators: HashMap::new(),
            _marker: PhantomData,
        }
    }

    fn extract_keys(&self, tuple: &Tuple) -> Vec<Field> {
        let mut grouped_keys: Vec<Field> = Vec::new();
        for key in &self.group_keys {
            for column in &self.inputs.get_columns() {
                let t: Target = key.clone();

                if t.table_name.is_some() {
                    if t.table_name.unwrap() != column.table_name {
                        continue;
                    }
                }

                if t.name == column.name {
                    grouped_keys.push(tuple.fields[column.offset].clone());
                }
            }
        }
        grouped_keys
    }

    fn upsert(&mut self, keys: Vec<Field>, tuple: Tuple) {
        if !self.grouped_aggregators.contains_key(&keys) {
            let init_aggrs: Vec<Aggregator> = self.aggregators.iter().map(|a| a.clone()).collect();
            self.grouped_aggregators
                .insert(keys.clone(), init_aggrs.clone());
        }

        {
            let aggrs = self.grouped_aggregators.get_mut(&keys).unwrap();
            for mut aggr in aggrs {
                let _ = aggr.update(&tuple, &self.inputs.get_columns());
            }
        }
    }
}

impl<'a, 't, T> Iterator for AggregationExec<'a, 't, T>
where
    T: ScanIterator,
{
    type Item = Vec<Tuple>;
    fn next(&mut self) -> Option<Vec<Tuple>> {
        loop {
            match self.inputs.next() {
                None => return None,
                Some(tuple) => {
                    let mut grouped_keys: Vec<Field> = self.extract_keys(&tuple);
                    &mut self.upsert(grouped_keys.clone(), tuple);

                    let mut tuples: Vec<Tuple> = Vec::new();
                    for (keys, aggrs) in &mut self.grouped_aggregators.iter_mut() {
                        let mut fields: Vec<Field> = keys.clone();
                        for aggr in aggrs {
                            fields.push(aggr.fetch_result());
                        }
                        tuples.push(Tuple::new(fields));
                    }
                    return Some(tuples);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {}
