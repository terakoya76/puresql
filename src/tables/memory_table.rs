use std::collections::Bound::Included;
use std::collections::BTreeMap;

use columns::column::Column;
use Field;
use Tuple;
use meta::table_info::TableInfo;

#[derive(Debug, Clone)]
pub struct MemoryTable {
    pub id: usize,
    pub name: String,
    pub columns: Vec<Column>,
    pub meta: TableInfo,
    pub tree: BTreeMap<usize, Tuple>,
}

impl MemoryTable {
    pub fn new(meta: TableInfo) -> MemoryTable {
        let mut columns: Vec<Column> = Vec::new();
        for column_info in &meta.columns {
            columns.push(column_info.to_column(&meta.name));
        }

        MemoryTable {
            id: meta.id,
            name: meta.name.clone(),
            columns: columns,
            tree: BTreeMap::new(),
            meta: meta,
        }
    }

    // TODO: IMPL some response as API
    pub fn insert(&mut self, fields: Vec<Field>) {
        let record_id = self.meta.next_record_id.base;
        let tuple: Tuple = Tuple::new(fields);
        &mut self.tree.insert(record_id, tuple);
        &mut self.meta.next_record_id.increment();
    }

    // TODO: return MUTABLE REF not clone
    pub fn get_tuple(&mut self, internal_id: usize) -> Tuple {
        match self.tree.get_mut(&internal_id) {
            None => Tuple::new(vec![]),
            Some(t) => t.clone(),
        }
    }

    pub fn seek(&self, current_handle: usize) -> Option<usize> {
        let offset: usize = self.tree.len();
        if current_handle > offset {
            return None;
        }

        match self.tree
            .range((Included(&current_handle), Included(&offset)))
            .next()
        {
            None => self.seek(current_handle + 1),
            Some(node) => Some(node.0.clone()),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum MemoryTableError {
    StorageFileNotFoundError,
}

#[cfg(test)]
mod tests {}
