use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;

pub trait Aggregator {
    fn update(&mut self, tuple: &Tuple, columns: &Vec<Column>);
    fn fetch_result(&self) -> Field;
    fn box_clone(&self) -> Box<Aggregator>;
}

impl Clone for Box<Aggregator> {
    fn clone(&self) -> Box<Aggregator> {
        self.box_clone()
    }
}

#[derive(Clone)]
pub struct AggrCount {
    pub result: Field,
}

impl AggrCount {
    pub fn new() -> Box<AggrCount> {
        Box::new(
            AggrCount {
                result: Field::set_init(),
            }
        )
    }
}

impl Aggregator for AggrCount {
    fn update(&mut self, tuple: &Tuple, columns: &Vec<Column>) {
        let next_value: Field = self.result.clone() + Field::set_u64(1);
        self.result = next_value;
    }

    fn fetch_result(&self) -> Field {
        self.result.clone()
    }

    fn box_clone(&self) -> Box<Aggregator> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
pub struct AggrSum {
    pub result: Field,
    pub column_name: String,
}

impl AggrSum {
    pub fn new(col_name: &str) -> Box<AggrSum> {
        Box::new(
            AggrSum {
                result: Field::set_init(),
                column_name: col_name.to_string(),
            }
        )
    }
}

impl Aggregator for AggrSum {
    fn update(&mut self, tuple: &Tuple, columns: &Vec<Column>) {
        for column in columns {
            if column.name == self.column_name {
                let value: Field = tuple.fields[column.offset].clone();
                self.result = self.result.clone() + value;
                return;
            }
        }
        self.result = self.result.clone();
    }

    fn fetch_result(&self) -> Field {
        self.result.clone()
    }

    fn box_clone(&self) -> Box<Aggregator> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
pub struct AggrAvg {
    pub sum: Field,
    pub iterate_num: usize,
    pub column_name: String,
}

impl AggrAvg {
    pub fn new(col_name: &str) -> Box<AggrAvg> {
        Box::new(
            AggrAvg {
                sum: Field::set_init(),
                iterate_num: 0,
                column_name: col_name.to_string(),
            }
        )
    }
}

impl Aggregator for AggrAvg {
    fn update(&mut self, tuple: &Tuple, columns: &Vec<Column>) {
        self.iterate_num += 1;
        for column in columns {
            if column.name == self.column_name {
                let value: Field = tuple.fields[column.offset].clone();
                self.sum = self.sum.clone() + value;
                return;
            }
        }
        self.sum = self.sum.clone();
    }

    fn fetch_result(&self) -> Field {
        self.sum.clone() / self.sum.set_same_type(self.iterate_num)
    }

    fn box_clone(&self) -> Box<Aggregator> {
        Box::new(self.clone())
    }
}

