use column::column::Column;
use meta::index_info::IndexInfo;
use allocator::allocator::Allocator;

#[derive(Clone)]
pub struct TableInfo {
    pub id: usize,
    pub name: String,
    pub columns: Vec<Column>,
    pub indices: Vec<IndexInfo>,
    pub next_record_id: Box<Allocator>,
}

impl TableInfo {
    pub fn new(alloc: &mut Box<Allocator>, name: &str, column_names: Vec<&str>, indices: Vec<IndexInfo>) -> TableInfo {
        let table_id: usize = alloc.base;
        alloc.increament();

        let mut columns: Vec<Column> = Vec::new();
        for (i, column_name) in column_names.iter().enumerate() {
            columns.push(Column::new(name, column_name, i));
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

    /*
    fn column_is_indexed(&self, column: Column) -> bool {
        for index in &self.indices {
            for indexed_column in &index.clone().columns {
                if indexed_column.name == column.name {
                    return true;
                }
            }
        }
        false
    }
    */
}

