use meta::column_info::ColumnInfo;
use meta::index_info::IndexInfo;
use allocator::allocator::Allocator;

#[derive(Clone)]
pub struct TableInfo {
    pub id: usize,
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub indices: Vec<IndexInfo>,
    pub next_record_id: Box<Allocator>,
}

impl TableInfo {
    pub fn new(alloc: &mut Box<Allocator>, name: &str, column_names: Vec<&str>, indices: Vec<IndexInfo>) -> TableInfo {
        let table_id: usize = alloc.base;
        alloc.increament();

        let mut columns: Vec<ColumnInfo> = Vec::new();
        for (i, column_name) in column_names.iter().enumerate() {
            columns.push(ColumnInfo::new(column_name, i));
        }

        let alloc: Box<Allocator> = Allocator::new(table_id);

        TableInfo {
            id: table_id,
            name: name.to_string(),
            columns: columns,
            indices: indices,
            next_record_id: alloc,
        }
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

