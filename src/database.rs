use std::collections::HashMap;

use meta::table_info::TableInfo;
use tables::memory_table::MemoryTable;

#[derive(Debug, Clone)]
pub struct Database {
    pub id: usize,
    pub name: String,
    pub tables: HashMap<String, TableInfo>,
}

impl Database {
    pub fn add_table(&mut self, table_info: TableInfo) {
        let name: String = table_info.name.clone();
        self.tables.insert(name, table_info);
    }

    // TODO: impl MemoryTable::load and replace #new
    pub fn load_table(&mut self, name: &str) -> Result<MemoryTable, ()> {
        match self.tables.get_mut(name) {
            None => Err(()),
            Some(tbl_info) => Ok(try!(MemoryTable::new(tbl_info))),
        }
    }
}

