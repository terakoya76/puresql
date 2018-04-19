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
        self.tables.insert(table_info.name.clone(), table_info);
    }

    pub fn table_info_from_str(&self, name: &str) -> Result<TableInfo, DatabaseError> {
        match self.tables.get(name) {
            None => Err(DatabaseError::TableNotFoundError),
            Some(tbl_info) => Ok(tbl_info.clone()),
        }
    }

    pub fn table_infos_from_str(&mut self, names: &[String]) -> Result<Vec<TableInfo>, DatabaseError> {
        let mut tables: Vec<TableInfo> = Vec::new();
        for name in names {
            match self.tables.get_mut(&name[..]) {
                None => return Err(DatabaseError::TableNotFoundError),
                Some(tbl_info) => tables.push(tbl_info.clone()),
            }
        }

        Ok(tables)
    }

    pub fn load_table(&mut self, name: String) -> Result<MemoryTable, DatabaseError> {
        match self.tables.get_mut(&name) {
            None => Err(DatabaseError::TableNotFoundError),
            Some(tbl_info) => {
                let mem_tbl: MemoryTable = try!(MemoryTable::new(tbl_info.clone()));
                Ok(mem_tbl)
            },
        }
    }

    pub fn load_tables(&mut self, names: &[String]) -> Result<Vec<MemoryTable>, DatabaseError> {
        let mut tables: Vec<MemoryTable> = Vec::new();
        for name in names {
            let tbl_info: &mut TableInfo = match self.tables.get_mut(&name[..]) {
                None => return Err(DatabaseError::TableNotFoundError),
                Some(tbl_info) => tbl_info,
            };

            tables.push(try!(MemoryTable::new(tbl_info.clone())));
        }

        Ok(tables)
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
