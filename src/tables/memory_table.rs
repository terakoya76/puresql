use std::collections::BTreeMap;
use std::collections::Bound::Included;

use meta::table_info::TableInfo;
use field::field::Field;
use column::column::Column;
use tuple::tuple::Tuple;

pub struct MemoryTable<'t> {
    pub id: usize,
    pub name: String,
    pub columns: Vec<Column>,
    pub tree: BTreeMap<usize, Tuple>,
    pub meta: &'t mut TableInfo,
}

impl<'t> MemoryTable<'t> {
    pub fn new(meta: &'t mut TableInfo) -> MemoryTable<'t> {
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
        let internal_id: usize = self.meta.next_record_id.base;
        if !self.tree.contains_key(&internal_id) {
            let tuple: Tuple = Tuple::new(fields);
            &mut self.tree.insert(internal_id, tuple);
            &mut self.meta.next_record_id.increament();
        }
    }

    pub fn get_tuple(&self, internal_id: usize) -> Tuple {
        match self.tree.get(&internal_id) {
            None => Tuple::new(vec![]),
            Some(tuple) => tuple.clone(),
        }
    }

    pub fn seek(&self, current_handle: usize) -> Option<usize> {
        let offset: usize = self.tree.len();
        if current_handle > offset {
            return None;
        }
        match self.tree.range((Included(&current_handle), Included(&offset))).next() {
            None => self.seek(current_handle+1),
            Some(node) => Some(node.0.clone()),
        }
    }

    pub fn print(&self) {
        let mut col_buffer: String = String::new();
        for col in &self.columns {
            col_buffer += "|";
            col_buffer += &col.name;
        }
        println!("{}", col_buffer);

        for tuple in self.tree.values() {
            tuple.print();
        }
    }
}

