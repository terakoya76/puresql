use meta::table_info::TableInfo;

#[derive(Debug, Clone)]
pub struct Database {
    pub id: usize,
    pub name: String,
    pub tables: Vec<TableInfo>,
}

impl Database {
    pub fn add_table(&mut self, table_info: TableInfo) {
        self.tables.push(table_info);
    }
}

