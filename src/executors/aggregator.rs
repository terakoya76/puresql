use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;
use parser::statement::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Aggregators {
    Count,
    Sum,
    Average,
    Max,
    Min,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Aggregator {
    pub kind: Aggregators,
    pub table: Option<String>,
    pub column: String,
    pub result: Field,
    pub iterate_num: usize,
}

impl Aggregator {
    pub fn update(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        self.iterate_num += 1;
        match self.kind {
            Aggregators::Count => self.update_count(tuple, columns),
            Aggregators::Sum => self.update_sum(tuple, columns),
            Aggregators::Average => self.update_average(tuple, columns),
            Aggregators::Max => self.update_max(tuple, columns),
            Aggregators::Min => self.update_min(tuple, columns),
        }
    }

    pub fn fetch_result(&self) -> Field {
        match self.kind {
            Aggregators::Count => self.result.clone(),
            Aggregators::Sum => self.result.clone(),
            Aggregators::Average => {
                self.result.clone() / self.result.set_same_type(self.iterate_num)
            }
            Aggregators::Max => self.result.clone(),
            Aggregators::Min => self.result.clone(),
        }
    }

    fn update_count(&mut self, _tuple: &Tuple, _columns: &[Column]) -> Result<(), AggregatorError> {
        let next_value: Field = self.result.clone() + Field::set_u64(1);
        self.result = next_value;
        Ok(())
    }

    fn update_sum(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(self.find_field(tuple, columns));
        self.result = self.result.clone() + value;
        Ok(())
    }

    fn update_average(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(self.find_field(tuple, columns));
        self.result = self.result.clone() + value;
        Ok(())
    }

    fn update_max(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(self.find_field(tuple, columns));
        if self.result < value {
            self.result = value;
        }
        Ok(())
    }

    fn update_min(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(self.find_field(tuple, columns));
        if self.result > value {
            self.result = value;
        }
        Ok(())
    }

    fn find_field(&self, tuple: &Tuple, columns: &[Column]) -> Result<Field, AggregatorError> {
        for column in columns {
            if self.column != column.name {
                continue;
            }

            match self.table {
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
}

pub fn build_aggregator(aggregate: Aggregate) -> Result<Aggregator, AggregatorError> {
    match aggregate {
        Aggregate::Count(aggr) => match aggr {
            Aggregatable::Target(target) => Ok(Aggregator {
                kind: Aggregators::Count,
                table: target.table_name.clone(),
                column: target.name.clone(),
                result: Field::set_init(),
                iterate_num: 0,
            }),
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Sum(aggr) => match aggr {
            Aggregatable::Target(target) => Ok(Aggregator {
                kind: Aggregators::Sum,
                table: target.table_name.clone(),
                column: target.name.clone(),
                result: Field::set_init(),
                iterate_num: 0,
            }),
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Average(aggr) => match aggr {
            Aggregatable::Target(target) => Ok(Aggregator {
                kind: Aggregators::Average,
                table: target.table_name.clone(),
                column: target.name.clone(),
                result: Field::set_init(),
                iterate_num: 0,
            }),
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Max(aggr) => match aggr {
            Aggregatable::Target(target) => Ok(Aggregator {
                kind: Aggregators::Max,
                table: target.table_name.clone(),
                column: target.name.clone(),
                result: Field::set_init(),
                iterate_num: 0,
            }),
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Min(aggr) => match aggr {
            Aggregatable::Target(target) => Ok(Aggregator {
                kind: Aggregators::Min,
                table: target.table_name.clone(),
                column: target.name.clone(),
                result: Field::set_init(),
                iterate_num: 0,
            }),
            _ => Err(AggregatorError::UnexpectedTargetError),
        },
    }
}

#[derive(Debug, PartialEq)]
pub enum AggregatorError {
    ColumnNotFoundError,
    UnexpectedTargetError,
}

#[cfg(test)]
mod tests {}
