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
            Aggregators::Average => if self.result == Field::set_i64(0) {
                self.result.clone()
            } else {
                self.result.clone() / self.result.set_same_type(self.iterate_num)
            }
            Aggregators::Max => self.result.clone(),
            Aggregators::Min => self.result.clone(),
        }
    }

    fn update_count(&mut self, _tuple: &Tuple, _columns: &[Column]) -> Result<(), AggregatorError> {
        let next_value: Field = self.result.clone() + Field::set_i64(1);
        self.result = next_value;
        Ok(())
    }

    fn update_sum(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(self.find_field(tuple, columns));
        if !value.aggregatable() {
            return Err(AggregatorError::UnexpectedTargetError)
        }

        if self.result.kind == value.kind {
            self.result = self.result.clone() + value;
        } else {
            self.result = value
        }
        Ok(())
    }

    fn update_average(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(self.find_field(tuple, columns));
        if !value.aggregatable() {
            return Err(AggregatorError::UnexpectedTargetError)
        }

        if self.result.kind == value.kind {
            self.result = self.result.clone() + value;
        } else {
            self.result = value
        }
        Ok(())
    }

    fn update_max(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(self.find_field(tuple, columns));
        if !value.aggregatable() {
            return Err(AggregatorError::UnexpectedTargetError)
        }

        if self.result.kind == value.kind {
            if self.result < value {
                self.result = value;
            }
        } else {
            if value.set_same_type(0) < value {
                self.result = value;
            }
        }
        Ok(())
    }

    fn update_min(&mut self, tuple: &Tuple, columns: &[Column]) -> Result<(), AggregatorError> {
        let value: Field = try!(self.find_field(tuple, columns));
        if !value.aggregatable() {
            return Err(AggregatorError::UnexpectedTargetError)
        }

        if self.result.kind == value.kind {
            if self.result > value {
                self.result = value;
            }
        } else {
            if value.set_same_type(0) > value {
                self.result = value;
            }
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
                result: Field::set_i64(0),
                iterate_num: 0,
            }),
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Sum(aggr) => match aggr {
            Aggregatable::Target(target) => Ok(Aggregator {
                kind: Aggregators::Sum,
                table: target.table_name.clone(),
                column: target.name.clone(),
                result: Field::set_i64(0),
                iterate_num: 0,
            }),
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Average(aggr) => match aggr {
            Aggregatable::Target(target) => Ok(Aggregator {
                kind: Aggregators::Average,
                table: target.table_name.clone(),
                column: target.name.clone(),
                result: Field::set_i64(0),
                iterate_num: 0,
            }),
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Max(aggr) => match aggr {
            Aggregatable::Target(target) => Ok(Aggregator {
                kind: Aggregators::Max,
                table: target.table_name.clone(),
                column: target.name.clone(),
                result: Field::set_i64(0),
                iterate_num: 0,
            }),
            _ => Err(AggregatorError::UnexpectedTargetError),
        },

        Aggregate::Min(aggr) => match aggr {
            Aggregatable::Target(target) => Ok(Aggregator {
                kind: Aggregators::Min,
                table: target.table_name.clone(),
                column: target.name.clone(),
                result: Field::set_i64(0),
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
mod tests {
    use super::*;
    use data_type::DataType;

    fn gen_target() -> Target {
        Target {
            table_name: Some("shohin".to_owned()),
            name: "price".to_owned(),
        }
    }

    fn gen_columns() -> Vec<Column> {
        let column_defs = &[
            ("shohin_id".to_owned(), DataType::Int),
            ("shohin_name".to_owned(), DataType::Char(10)),
            ("price".to_owned(), DataType::Int),
        ];

        column_defs
            .into_iter()
            .enumerate()
            .map(|(i, col)| Column {
                table_name: "shohin".to_owned(),
                name: col.clone().0,
                dtype: col.clone().1,
                offset: i,
            })
            .collect()
    }

    fn gen_int_tuple() -> Tuple {
        Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
        ])
    }

    fn gen_float_tuple() -> Tuple {
        Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_f64(300.5),
        ])
    }

    #[test]
    fn test_build_count() {
        let target: Target = gen_target();
        let aggregatable = Aggregate::Count(Aggregatable::Target(target.clone()));
        let aggregator = build_aggregator(aggregatable).unwrap();

        let count = Aggregator {
            kind: Aggregators::Count,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };

        assert_eq!(aggregator, count);
    }

    #[test]
    fn test_update_int_count() {
        let target: Target = gen_target();
        let mut count = Aggregator {
            kind: Aggregators::Count,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(count.fetch_result(), Field::set_i64(0));

        let _ = count.update(&gen_int_tuple(), &gen_columns());
        assert_eq!(count.fetch_result(), Field::set_i64(1));

        let _ = count.update(&gen_int_tuple(), &gen_columns());
        assert_eq!(count.fetch_result(), Field::set_i64(2));
    }

    #[test]
    fn test_update_float_count() {
        let target: Target = gen_target();
        let mut count = Aggregator {
            kind: Aggregators::Count,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(count.fetch_result(), Field::set_i64(0));

        let _ = count.update(&gen_float_tuple(), &gen_columns());
        assert_eq!(count.fetch_result(), Field::set_i64(1));

        let _ = count.update(&gen_float_tuple(), &gen_columns());
        assert_eq!(count.fetch_result(), Field::set_i64(2));
    }

    #[test]
    fn test_update_str_count() {
        let target: Target = Target {
            table_name: Some("shohin".to_owned()),
            name: "shohin_name".to_owned(),
        };
        let mut count = Aggregator {
            kind: Aggregators::Count,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(count.fetch_result(), Field::set_i64(0));

        let _ = count.update(&gen_int_tuple(), &gen_columns());
        assert_eq!(count.fetch_result(), Field::set_i64(1));

        let _ = count.update(&gen_int_tuple(), &gen_columns());
        assert_eq!(count.fetch_result(), Field::set_i64(2));
    }

    #[test]
    fn test_build_sum() {
        let target: Target = gen_target();
        let aggregatable = Aggregate::Sum(Aggregatable::Target(target.clone()));
        let aggregator = build_aggregator(aggregatable).unwrap();

        let sum = Aggregator {
            kind: Aggregators::Sum,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };

        assert_eq!(aggregator, sum);
    }

    #[test]
    fn test_update_int_sum() {
        let target: Target = gen_target();
        let mut sum = Aggregator {
            kind: Aggregators::Sum,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(sum.fetch_result(), Field::set_i64(0));

        let _ = sum.update(&gen_int_tuple(), &gen_columns());
        assert_eq!(sum.fetch_result(), Field::set_i64(300));

        let _ = sum.update(&gen_int_tuple(), &gen_columns());
        assert_eq!(sum.fetch_result(), Field::set_i64(600));
    }

    #[test]
    fn test_update_float_sum() {
        let target: Target = gen_target();
        let mut sum = Aggregator {
            kind: Aggregators::Sum,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(sum.fetch_result(), Field::set_i64(0));

        let _ = sum.update(&gen_float_tuple(), &gen_columns());
        assert_eq!(sum.fetch_result(), Field::set_f64(300.5));

        let _ = sum.update(&gen_float_tuple(), &gen_columns());
        assert_eq!(sum.fetch_result(), Field::set_f64(601.0));
    }

    #[test]
    fn test_update_str_sum() {
        let target: Target = Target {
            table_name: Some("shohin".to_owned()),
            name: "shohin_name".to_owned(),
        };
        let mut sum = Aggregator {
            kind: Aggregators::Sum,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(sum.fetch_result(), Field::set_i64(0));
        assert_eq!(
            sum.update(&gen_int_tuple(), &gen_columns()).is_err(),
            true
        );
    }

    #[test]
    fn test_build_average() {
        let target: Target = gen_target();
        let aggregatable = Aggregate::Average(Aggregatable::Target(target.clone()));
        let aggregator = build_aggregator(aggregatable).unwrap();

        let avg = Aggregator {
            kind: Aggregators::Average,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };

        assert_eq!(aggregator, avg);
    }

    #[test]
    fn test_update_int_average() {
        let target: Target = gen_target();
        let mut avg = Aggregator {
            kind: Aggregators::Average,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(avg.fetch_result(), Field::set_i64(0));

        let _ = avg.update(&gen_int_tuple(), &gen_columns());
        assert_eq!(avg.fetch_result(), Field::set_i64(300));

        let _ = avg.update(&gen_int_tuple(), &gen_columns());
        assert_eq!(avg.fetch_result(), Field::set_i64(300));
    }

    #[test]
    fn test_update_float_avg() {
        let target: Target = gen_target();
        let mut avg = Aggregator {
            kind: Aggregators::Average,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(avg.fetch_result(), Field::set_i64(0));

        let _ = avg.update(&gen_float_tuple(), &gen_columns());
        assert_eq!(avg.fetch_result(), Field::set_f64(300.5));

        let _ = avg.update(&gen_float_tuple(), &gen_columns());
        assert_eq!(avg.fetch_result(), Field::set_f64(300.5));
    }

    #[test]
    fn test_update_str_avg() {
        let target: Target = Target {
            table_name: Some("shohin".to_owned()),
            name: "shohin_name".to_owned(),
        };
        let mut avg = Aggregator {
            kind: Aggregators::Average,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(avg.fetch_result(), Field::set_i64(0));
        assert_eq!(
            avg.update(&gen_int_tuple(), &gen_columns()).is_err(),
            true
        );
    }

    #[test]
    fn test_build_max() {
        let target: Target = gen_target();
        let aggregatable = Aggregate::Max(Aggregatable::Target(target.clone()));
        let aggregator = build_aggregator(aggregatable).unwrap();

        let max = Aggregator {
            kind: Aggregators::Max,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };

        assert_eq!(aggregator, max);
    }

    #[test]
    fn test_update_int_max() {
        let target: Target = gen_target();
        let mut max = Aggregator {
            kind: Aggregators::Max,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(max.fetch_result(), Field::set_i64(0));

        let _ = max.update(&gen_int_tuple(), &gen_columns());
        assert_eq!(max.fetch_result(), Field::set_i64(300));

        let tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(100),
        ]);
        let _ = max.update(&tuple, &gen_columns());
        assert_eq!(max.fetch_result(), Field::set_i64(300));
    }

    #[test]
    fn test_update_float_max() {
        let target: Target = gen_target();
        let mut max = Aggregator {
            kind: Aggregators::Max,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(max.fetch_result(), Field::set_i64(0));

        let _ = max.update(&gen_float_tuple(), &gen_columns());
        assert_eq!(max.fetch_result(), Field::set_f64(300.5));

        let tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_f64(100.5),
        ]);
        let _ = max.update(&tuple, &gen_columns());
        assert_eq!(max.fetch_result(), Field::set_f64(300.5));
    }

    #[test]
    fn test_update_str_max() {
        let target: Target = Target {
            table_name: Some("shohin".to_owned()),
            name: "shohin_name".to_owned(),
        };
        let mut max = Aggregator {
            kind: Aggregators::Max,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(max.fetch_result(), Field::set_i64(0));
        assert_eq!(
            max.update(&gen_int_tuple(), &gen_columns()).is_err(),
            true
        );
    }

    #[test]
    fn test_build_min() {
        let target: Target = gen_target();
        let aggregatable = Aggregate::Min(Aggregatable::Target(target.clone()));
        let aggregator = build_aggregator(aggregatable).unwrap();

        let min = Aggregator {
            kind: Aggregators::Min,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };

        assert_eq!(aggregator, min);
    }

    #[test]
    fn test_update_int_min() {
        let target: Target = gen_target();
        let mut min = Aggregator {
            kind: Aggregators::Min,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(min.fetch_result(), Field::set_i64(0));

        let tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(-100),
        ]);
        let _ = min.update(&tuple, &gen_columns());
        assert_eq!(min.fetch_result(), Field::set_i64(-100));

        let tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(-50),
        ]);
        let _ = min.update(&tuple, &gen_columns());
        assert_eq!(min.fetch_result(), Field::set_i64(-100));
    }

    #[test]
    fn test_update_float_min() {
        let target: Target = gen_target();
        let mut min = Aggregator {
            kind: Aggregators::Min,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(min.fetch_result(), Field::set_i64(0));

        let tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_f64(-300.5),
        ]);
        let _ = min.update(&tuple, &gen_columns());
        assert_eq!(min.fetch_result(), Field::set_f64(-300.5));

        let tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_f64(-100.5),
        ]);
        let _ = min.update(&tuple, &gen_columns());
        assert_eq!(min.fetch_result(), Field::set_f64(-300.5));
    }

    #[test]
    fn test_update_str_min() {
        let target: Target = Target {
            table_name: Some("shohin".to_owned()),
            name: "shohin_name".to_owned(),
        };
        let mut min = Aggregator {
            kind: Aggregators::Min,
            table: target.table_name.clone(),
            column: target.name.clone(),
            result: Field::set_i64(0),
            iterate_num: 0,
        };
        assert_eq!(min.fetch_result(), Field::set_i64(0));
        assert_eq!(
            min.update(&gen_int_tuple(), &gen_columns()).is_err(),
            true
        );
    }
}
