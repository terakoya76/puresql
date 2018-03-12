use column::column::Column;
use meta::table_info::TableInfo;

#[derive(Clone)]
pub struct IndexInfo {
    pub id: usize,
    pub name: String,
    pub table_name: String,
    pub columns: Vec<Column>,
}

impl IndexInfo {
    fn new(table_info: &TableInfo, name: &str, columns: Vec<Column>) -> IndexInfo {
        IndexInfo {
            id: 0,
            name: name.to_string(),
            table_name: table_info.name.clone(),
            columns: columns,
        }
    }
}

