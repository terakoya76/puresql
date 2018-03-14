use column::column::Column;

#[derive(Clone)]
pub struct ColumnInfo {
    pub id: usize,
    pub name: String,
    pub offset: usize,
}

impl ColumnInfo {
    pub fn new(name: &str, offset: usize) -> ColumnInfo {
        ColumnInfo {
            id: 0,
            name: name.to_string(),
            offset: offset,
        }
    }

    pub fn to_column(&self, table_name: &str) -> Column {
        Column {
            table_name: table_name.to_string(),
            name: self.name.clone(),
            offset: self.offset,
        }
    }
}

