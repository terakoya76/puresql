use columns::column::Column;
use tables::tuple::Tuple;

pub trait ScanIterator : Iterator<Item=Tuple> {
    fn get_columns(&self) -> Vec<Column>;
}

