use column::column::Column;
use tuple::tuple::Tuple;
use field::field::Field;

pub struct Aggregator {
    pub result: Field,
    pub function: Box<Fn(usize, &Field, &Tuple, &Vec<Column>) -> Field>,
}

impl Aggregator {
    pub fn update(&mut self, next_value: Field) {
        self.result = next_value;
    }

    pub fn count() -> Aggregator {
        let agg_func = |i: usize, result: &Field, tuple: &Tuple, columns: &Vec<Column>| -> Field {
            result.clone() + Field::set_u64(1)
        };

        Aggregator {
            result: Field::set_init(),
            function: Box::new(agg_func),
        }
    }

    pub fn sum(col_name: &str) -> Aggregator {
        let deref_col_name: String = col_name.to_string();
        let agg_func = move |i: usize, result: &Field, tuple: &Tuple, columns: &Vec<Column>| {
            for column in columns {
                if column.name == deref_col_name {
                    let value: Field = tuple.fields[column.offset].clone();
                    return result.clone() + value;
                }
            }
            result.clone()
        };

        Aggregator {
            result: Field::set_init(),
            function: Box::new(agg_func),
        }
    }

    /*
    pub fn average(col_name: &str) -> Aggregator {
        let deref_col_name: String = col_name.to_string();
        let agg_func = move |i: usize, result: &Field, tuple: &Tuple, columns: &Vec<Column>| {
            for column in columns {
                if column.name == deref_col_name {
                    let value: Field = tuple.fields[column.offset].clone();
                    result.set_same_type(i).to_string();
                    return (result.clone() + value) / result.set_same_type(i);
                }
            }
            result.clone() / result.set_same_type(i)
        };

        Aggregator {
            result: Field::set_init(),
            function: Box::new(agg_func),
        }
    }
    */
}
