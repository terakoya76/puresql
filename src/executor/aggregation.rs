use std::collections::HashMap;

use tuple::tuple::Tuple;
use field::field::Field;
use executor::memory_table_scan::MemoryTableScanExec;
use executor::aggregator::Aggregator;

pub struct AggregationExec<'a, 'ts: 'a, 't: 'ts> {
    pub group_keys: Vec<String>,
    pub inputs: &'a mut MemoryTableScanExec<'ts, 't>,
    pub aggregators: Vec<Box<Aggregator>>,
    pub grouped_aggregators: HashMap<Vec<String>, Vec<Box<Aggregator>>>,
}

impl<'a, 'ts, 't> AggregationExec<'a, 'ts, 't> {
    pub fn new(inputs: &'a mut MemoryTableScanExec<'ts, 't>, group_keys: Vec<&str>, aggregators: Vec<Box<Aggregator>>) -> AggregationExec<'a, 'ts, 't> {
        AggregationExec {
            group_keys: group_keys.iter().map(|k| k.to_string()).collect(),
            inputs: inputs,
            aggregators: aggregators,
            grouped_aggregators: HashMap::new(),
        }
    }

    fn get_keys(&self, tuple: &Tuple) -> Vec<String> {
        let mut map_keys: Vec<String> = Vec::new();
        for key in &self.group_keys {
            for column in &self.inputs.columns {
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
                aggr.update(&tuple, &self.inputs.columns);
            }
        }
    }
}

impl<'a, 'ts, 't> Iterator for AggregationExec<'a, 'ts, 't> {
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

