use std::collections::HashMap;

use meta::table_info::TableInfo;
use tables::memory_table::{MemoryTable, MemoryTableError};

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

    pub fn table_info_from_str(&self, name: &str) -> Result<TableInfo, DatabaseError> {
        match self.tables.get(name) {
            None => Err(DatabaseError::TableNotFoundError),
            Some(tbl_info) => Ok(tbl_info.clone()),
        }
    }

    pub fn load_table(&mut self, name: &str) -> Result<MemoryTable, DatabaseError> {
        match self.tables.get_mut(name) {
            None => Err(DatabaseError::TableNotFoundError),
            Some(tbl_info) => Ok(try!(MemoryTable::new(tbl_info))),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum DatabaseError {
    MemoryTableError(MemoryTableError),
    TableNotFoundError,
}

impl From<MemoryTableError> for DatabaseError {
    fn from(err: MemoryTableError) -> DatabaseError {
        DatabaseError::MemoryTableError(err)
    }
}

