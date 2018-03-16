use std::marker::PhantomData;
use std::collections::HashMap;

// trait
use ScanExec;

// struct
use Aggregator;
use tables::tuple::Tuple;
use tables::field::Field;

pub struct AggregationExec<'a, 't: 'a, T: 't> {
    pub group_keys: Vec<String>,
    pub inputs: &'a mut T,
    pub aggregators: Vec<Box<Aggregator>>,
    pub grouped_aggregators: HashMap<Vec<String>, Vec<Box<Aggregator>>>,
    _marker: PhantomData<&'t T>,
}

impl<'a, 't, T> AggregationExec<'a, 't, T>
    where T: ScanExec {
    pub fn new(inputs: &'a mut T, group_keys: Vec<&str>, aggregators: Vec<Box<Aggregator>>) -> AggregationExec<'a, 't, T> {
        AggregationExec {
            group_keys: group_keys.iter().map(|k| k.to_string()).collect(),
            inputs: inputs,
            aggregators: aggregators,
            grouped_aggregators: HashMap::new(),
            _marker: PhantomData,
        }
    }

    fn get_keys(&self, tuple: &Tuple) -> Vec<String> {
        let mut map_keys: Vec<String> = Vec::new();
        for key in &self.group_keys {
            for column in &self.inputs.get_columns() {
                if column.name == *key {
                    let value: String = tuple.fields[column.offset].clone().to_string();
                    map_keys.push(value);
                }
            }
        }
        map_keys
    }

    fn upsert(&mut self, keys: Vec<String>, tuple: Tuple) {
        if !self.grouped_aggregators.contains_key(&keys) {
            let init_aggrs: Vec<Box<Aggregator>> = self.aggregators.iter().map(|a| a.clone()).collect();
            self.grouped_aggregators.insert(keys.clone(), init_aggrs.clone());
        }

        {
            let aggrs = self.grouped_aggregators.get_mut(&keys).unwrap();
            for mut aggr in aggrs {
                aggr.update(&tuple, &self.inputs.get_columns());
            }
        }
    }
}

impl<'a, 't, T> Iterator for AggregationExec<'a, 't, T>
    where T: ScanExec {
    type Item = Vec<Tuple>;
    fn next(&mut self) -> Option<Vec<Tuple>> {
        loop {
            match self.inputs.next() {
                None => return None,
                Some(tuple) => {
                    let mut map_keys: Vec<String> = self.get_keys(&tuple);
                    &mut self.upsert(map_keys, tuple);

                    let mut tuples: Vec<Tuple> = Vec::new();
                    for aggrs in self.grouped_aggregators.values() {
                        let mut fields: Vec<Field> = Vec::new();
                        for aggr in aggrs {
                            fields.push(aggr.fetch_result());
                        }
                        tuples.push(Tuple::new(fields));
                    }
                    return Some(tuples);
                },
            }
        }
    }
}

