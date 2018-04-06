use columns::column::Column;
use tables::tuple::Tuple;

pub trait ScanExec : Iterator<Item=Tuple> {
    fn get_columns(&self) -> Vec<Column>;
    fn get_tuple(&mut self, handle: usize) -> Tuple;
    fn set_next_handle(&mut self, next_handle: usize);
    fn next_handle(&mut self) -> Option<usize>;
}

