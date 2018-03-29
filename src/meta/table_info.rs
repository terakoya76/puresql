use meta::column_info::ColumnInfo;
use meta::index_info::IndexInfo;
use allocators::allocator::Allocator;

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub id: usize,
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub indices: Vec<IndexInfo>,
    pub next_record_id: Box<Allocator>,
}

impl TableInfo {
    pub fn get_bin_path(&self) -> String {
        let mut path: String = "".to_string();
        path.push_str(&self.name);
        path.push_str(".txt");
        path
    }

    pub fn find_column_infos_by_names(&self, column_names: &Vec<&str>) -> Vec<ColumnInfo> {
        let mut columns: Vec<ColumnInfo> = Vec::new();
        for column_info in &self.columns {
            for column_name in column_names {
                if column_info.name == column_name.to_string() {
                    columns.push(column_info.clone());
                }
            }
        }
        columns
    }
}

