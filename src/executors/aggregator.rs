use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;
use parser::statement::*;

pub trait Aggregator {
    fn update(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError>;
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
    fn update(&mut self, _tuple: &Tuple, _columns: &[Column]) -> Result<(), AggregatorError> {
        let next_value: Field = self.result.clone() + Field::set_u64(1);
        self.result = next_value;
        Ok(())
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
    pub table: Option<String>,
    pub column: String,
    pub result: Field,
}

impl Sum {
    pub fn new(table: Option<String>, column: String) -> Box<Sum> {
        Box::new(Sum {
            table: table,
            column: column,
            result: Field::set_init(),
        })
    }
}

impl Aggregator for Sum {
    fn update(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(find_field(
            tuple,
            columns,
            self.table.clone(),
            self.column.clone()
        ));
        self.result = self.result.clone() + value;
        Ok(())
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
    pub table: Option<String>,
    pub column: String,
    pub sum: Field,
    pub iterate_num: usize,
}

impl Average {
    pub fn new(table: Option<String>, column: String) -> Box<Average> {
        Box::new(Average {
            table: table,
            column: column,
            sum: Field::set_init(),
            iterate_num: 0,
        })
    }
}

impl Aggregator for Average {
    fn update(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        self.iterate_num += 1;
        let value: Field = try!(find_field(
            tuple,
            columns,
            self.table.clone(),
            self.column.clone()
        ));
        self.sum = self.sum.clone() + value;
        Ok(())
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
    pub table: Option<String>,
    pub column: String,
    pub result: Field,
}

impl Max {
    pub fn new(table: Option<String>, column: String) -> Box<Max> {
        Box::new(Max {
            table: table,
            column: column,
            result: Field::set_init(),
        })
    }
}

impl Aggregator for Max {
    fn update(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(find_field(
            tuple,
            columns,
            self.table.clone(),
            self.column.clone()
        ));
        if self.result < value {
            self.result = value;
        }
        Ok(())
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
    pub table: Option<String>,
    pub column: String,
    pub result: Field,
}

impl Min {
    pub fn new(table: Option<String>, column: String) -> Box<Min> {
        Box::new(Min {
            table: table,
            column: column,
            result: Field::set_init(),
        })
    }
}

impl Aggregator for Min {
    fn update(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(find_field(
            tuple,
            columns,
            self.table.clone(),
            self.column.clone()
        ));
        if self.result > value {
            self.result = value;
        }
        Ok(())
    }

    fn fetch_result(&self) -> Field {
        self.result.clone()
    }

    fn box_clone(&self) -> Box<Aggregator> {
        Box::new(self.clone())
    }
}

fn find_field(
    tuple: &Tuple,
    columns: &[Column],
    table_name: Option<String>,
    column_name: String,
) -> Result<Field, AggregatorError> {
    for column in columns {
        if column_name != column.name {
            continue;
        }

        match table_name {
            None => return Ok(tuple.fields[column.offset].clone()),
            Some(ref tbl_name) => {
                if tbl_name == &column.table_name {
                    return Ok(tuple.fields[column.offset].clone());
                }
            }
        }
    }
    Err(AggregatorError::ColumnNotFoundError)
}

pub fn build_aggregator(aggregate: Aggregate) -> Result<Box<Aggregator>, AggregatorError> {
    match aggregate {
        Aggregate::Count(_aggr) => Ok(Count::new()),

        Aggregate::Sum(aggr) => match aggr {
            Aggregatable::Target(target) => {
                Ok(Sum::new(target.table_name.clone(), target.name.clone()))
            }
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Average(aggr) => match aggr {
            Aggregatable::Target(target) => {
                Ok(Average::new(target.table_name.clone(), target.name.clone()))
            }
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Max(aggr) => match aggr {
            Aggregatable::Target(target) => {
                Ok(Max::new(target.table_name.clone(), target.name.clone()))
            }
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Min(aggr) => match aggr {
            Aggregatable::Target(target) => {
                Ok(Min::new(target.table_name.clone(), target.name.clone()))
            }
            _ => Err(AggregatorError::UnexpectedTargetError),
        },
    }
}

#[derive(Debug, PartialEq)]
pub enum AggregatorError {
    ColumnNotFoundError,
    UnexpectedTargetError,
}
