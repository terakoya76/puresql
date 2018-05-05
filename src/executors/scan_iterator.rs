use meta::table_info::TableInfo;
use columns::column::Column;
use tables::tuple::Tuple;

pub trait ScanIterator: Iterator<Item = Tuple> {
    fn get_meta(&self) -> TableInfo;
    fn get_columns(&self) -> Vec<Column>;
}

#[cfg(test)]
mod tests {}
