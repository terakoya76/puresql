use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;

pub fn equal(col_name: &str, field: Field) -> Box<Fn(&Tuple, &Vec<Column>) -> bool> {
    let deref_col_name: String = col_name.to_string();

    Box::new(
        move |tuple: &Tuple, columns: &Vec<Column>| {
            let mut _result: bool = false;
            for column in columns {
                if column.name == deref_col_name {
                    _result = tuple.fields[column.offset] == field;
                    break;
                }
            }
            _result
        }
    )
}

pub fn not_equal(col_name: &str, field: Field) -> Box<Fn(&Tuple, &Vec<Column>) -> bool> {
    let deref_col_name: String = col_name.to_string();

    Box::new(
        move |tuple: &Tuple, columns: &Vec<Column>| {
            let mut _result: bool = false;
            for column in columns {
                if column.name == deref_col_name {
                    _result = tuple.fields[column.offset] != field;
                    break;
                }
            }
            _result
        }
    )
}

pub fn lt(col_name: &str, field: Field) -> Box<Fn(&Tuple, &Vec<Column>) -> bool> {
    let deref_col_name: String = col_name.to_string();

    Box::new(
        move |tuple: &Tuple, columns: &Vec<Column>| {
            let mut _result: bool = false;
            for column in columns {
                if column.name == deref_col_name {
                    _result = tuple.fields[column.offset] < field;
                    break;
                }
            }
            _result
        }
    )
}

pub fn le(col_name: &str, field: Field) -> Box<Fn(&Tuple, &Vec<Column>) -> bool> {
    let deref_col_name: String = col_name.to_string();

    Box::new(
        move |tuple: &Tuple, columns: &Vec<Column>| {
            let mut _result: bool = false;
            for column in columns {
                if column.name == deref_col_name {
                    _result = tuple.fields[column.offset] <= field;
                    break;
                }
            }
            _result
        }
    )
}

pub fn gt(col_name: &str, field: Field) -> Box<Fn(&Tuple, &Vec<Column>) -> bool> {
    let deref_col_name: String = col_name.to_string();

    Box::new(
        move |tuple: &Tuple, columns: &Vec<Column>| {
            let mut _result: bool = false;
            for column in columns {
                if column.name == deref_col_name {
                    _result = tuple.fields[column.offset] > field;
                    break;
                }
            }
            _result
        }
    )
}

pub fn ge(col_name: &str, field: Field) -> Box<Fn(&Tuple, &Vec<Column>) -> bool> {
    let deref_col_name: String = col_name.to_string();

    Box::new(
        move |tuple: &Tuple, columns: &Vec<Column>| {
            let mut _result: bool = false;
            for column in columns {
                if column.name == deref_col_name {
                    _result = tuple.fields[column.offset] >= field;
                    break;
                }
            }
            _result
        }
    )
}

