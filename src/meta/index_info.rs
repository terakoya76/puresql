use meta::table_info::TableInfo;
use meta::column_info::ColumnInfo;

#[derive(Clone)]
pub struct IndexInfo {
    pub id: usize,
    pub name: String,
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
}

impl IndexInfo {
    pub fn new(table_info: &mut TableInfo, name: &str, column_names: Vec<&str>) -> IndexInfo {

        let index_info = IndexInfo {
            id: 0,
            name: name.to_string(),
            table_name: table_info.name.clone(),
            columns: table_info.get_column_infos_from_names(column_names),
        };

        table_info.indices.push(index_info.clone());
        index_info
    }
}

