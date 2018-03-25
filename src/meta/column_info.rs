use data_type::DataType;
use columns::column::Column;

#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub dtype: DataType,
    pub offset: usize,
}

impl ColumnInfo {
    pub fn to_column(&self, table_name: &str) -> Column {
        Column {
            table_name: table_name.to_string(),
            name: self.name.clone(),
            dtype: self.dtype.clone(),
            offset: self.offset,
        }
    }
}

