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

#[derive(Debug, Clone)]
pub struct Count {
    pub result: Field,
}

impl Count {
    pub fn new() -> Box<Count> {
        Box::new(Count {
            result: Field::set_init(),
        })
    }
}

impl Aggregator for Count {
    fn update(&mut self, _tuple: &Tuple, _columns: &Vec<Column>) {
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

#[derive(Debug, Clone)]
pub struct Sum {
    pub result: Field,
    pub column_name: String,
}

impl Sum {
    pub fn new(col_name: &str) -> Box<Sum> {
        Box::new(Sum {
            result: Field::set_init(),
            column_name: col_name.to_string(),
        })
    }
}

impl Aggregator for Sum {
    fn update(&mut self, tuple: &Tuple, columns: &Vec<Column>) {
        for column in columns {
            if column.name == self.column_name {
                let value: Field = tuple.fields[column.offset].clone();
                self.result = self.result.clone() + value;
                return;
            }
        }
        //self.result = self.result.clone();
    }

    fn fetch_result(&self) -> Field {
        self.result.clone()
    }

    fn box_clone(&self) -> Box<Aggregator> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct Average {
    pub sum: Field,
    pub iterate_num: usize,
    pub column_name: String,
}

impl Average {
    pub fn new(col_name: &str) -> Box<Average> {
        Box::new(Average {
            sum: Field::set_init(),
            iterate_num: 0,
            column_name: col_name.to_string(),
        })
    }
}

impl Aggregator for Average {
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

#[derive(Debug, Clone)]
pub struct Max {
    pub result: Field,
    pub column_name: String,
}

impl Max {
    pub fn new(col_name: &str) -> Box<Max> {
        Box::new(Max {
            result: Field::set_init(),
            column_name: col_name.to_string(),
        })
    }
}

impl Aggregator for Max {
    fn update(&mut self, tuple: &Tuple, columns: &Vec<Column>) {
        for column in columns {
            if column.name == self.column_name {
                let value: Field = tuple.fields[column.offset].clone();
                if self.result < value {
                    self.result = value;
                }
            }
        }
    }

    fn fetch_result(&self) -> Field {
        self.result.clone()
    }

    fn box_clone(&self) -> Box<Aggregator> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct Min {
    pub result: Field,
    pub column_name: String,
}

impl Min {
    pub fn new(col_name: &str) -> Box<Min> {
        Box::new(Min {
            result: Field::set_init(),
            column_name: col_name.to_string(),
        })
    }
}

impl Aggregator for Min {
    fn update(&mut self, tuple: &Tuple, columns: &Vec<Column>) {
        for column in columns {
            if column.name == self.column_name {
                let value: Field = tuple.fields[column.offset].clone();
                if self.result > value {
                    self.result = value;
                }
            }
        }
    }

    fn fetch_result(&self) -> Field {
        self.result.clone()
    }

    fn box_clone(&self) -> Box<Aggregator> {
        Box::new(self.clone())
    }
}
