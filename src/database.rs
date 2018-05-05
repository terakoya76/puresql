use std::collections::HashMap;

use meta::table_info::TableInfo;
use tables::memory_table::{MemoryTable, MemoryTableError};

#[derive(Debug, Clone)]
pub struct Database {
    pub id: usize,
    pub name: String,
    pub tables: HashMap<String, TableInfo>,
    pub real_tables: HashMap<String, MemoryTable>,
}

impl Database {
    pub fn add_table(&mut self, table_info: TableInfo) {
        self.tables
            .insert(table_info.name.clone(), table_info.clone());
        self.real_tables.insert(
            table_info.name.clone(),
            MemoryTable::new(table_info.clone()),
        );
    }

    pub fn table_info_from_str(&self, name: &str) -> Result<TableInfo, DatabaseError> {
        match self.tables.get(name) {
            None => Err(DatabaseError::TableNotFoundError),
            Some(tbl_info) => Ok(tbl_info.clone()),
        }
    }

    pub fn load_table(&mut self, name: String) -> Result<&mut MemoryTable, DatabaseError> {
        match self.real_tables.get_mut(&name) {
            None => Err(DatabaseError::TableNotFoundError),
            Some(mem_tbl) => Ok(mem_tbl),
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

#[cfg(test)]
mod tests {}
