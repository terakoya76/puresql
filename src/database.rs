use meta::table_info::TableInfo;

#[derive(Debug, Clone)]
pub struct Database {
    pub id: usize,
    pub name: String,
    pub tables: Vec<TableInfo>,
}

