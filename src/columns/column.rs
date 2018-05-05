use data_type::DataType;

#[derive(Debug, Clone)]
pub struct Column {
    pub table_name: String,
    pub name: String,
    pub dtype: DataType,
    pub offset: usize,
}

#[cfg(test)]
mod tests {}
