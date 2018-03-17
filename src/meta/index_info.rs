use meta::table_info::TableInfo;
use meta::column_info::ColumnInfo;

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub id: usize,
    pub name: String,
    pub table_name: String,
    pub columns: Vec<ColumnInfo>,
    pub is_pk_index: bool,
}

impl IndexInfo {
    pub fn new(table_info: &mut TableInfo, column_names: Vec<&str>, is_pk: bool) -> IndexInfo {

        let index_info = IndexInfo {
            id: 0,
            name: Self::generate_name(&column_names),
            table_name: table_info.name.clone(),
            columns: table_info.find_column_infos_by_names(&column_names),
            is_pk_index: is_pk,
        };

        table_info.indices.push(index_info.clone());
        index_info
    }

    pub fn generate_name(column_names: &Vec<&str>) -> String {
        let mut name: String = "".to_string();
        for column_name in column_names {
            name.push_str(column_name);
            name.push_str("_");
        }
        name.pop();
        name
    }
}

