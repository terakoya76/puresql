use std::collections::Bound::Included;

use field::field::Field;
use column::column::Column;
use tuple::tuple::Tuple;
use index::index::Index;
use indexed::indexed::Indexed;
use meta::table_info::TableInfo;

pub struct Table<'t, 'i: 't> {
    pub id: usize,
    pub name: String,
    pub columns: Vec<Column>,
    pub indices: Vec<&'t mut Index<'i>>,
    pub meta: &'t mut TableInfo,
}

impl<'t, 'i> Table<'t, 'i> {
    pub fn new(meta: &'t mut TableInfo, indices: Vec<&'t mut Index<'i>>) -> Table<'t, 'i> {
        let mut columns: Vec<Column> = Vec::new();
        for column_info in &meta.columns {
            columns.push(column_info.to_column(&meta.name));
        }

        Table {
            id: meta.id,
            name: meta.name.clone(),
            columns: columns,
            indices: indices,
            meta: meta,
        }
    }

    // TODO: IMPL some response as API
    pub fn insert(&mut self, fields: Vec<Field>) {
        let internal_id: usize = self.meta.next_record_id.base;
        for ref mut index in self.indices.iter_mut() {
            &mut index.insert(internal_id, Tuple::new(fields.clone()));
        }
        &mut self.meta.next_record_id.increament();
    }

    pub fn get_column_offset(&self, column_name: &str) -> Option<usize> {
        for column in &self.columns {
            if column.name == column_name {
                return Some(column.offset.clone());
            }
        }
        None
    }

    pub fn get_fields_by_columns(&self, internal_id: usize, columns: &Vec<Column>) -> Tuple {
        let mut fields = Vec::new();
        let indexed = self.indices[0].tree.get(&internal_id);
        if indexed.is_some() {
            for column in columns {
                fields.push(indexed.unwrap().value.fields[column.offset].clone());
            }
        }
        Tuple::new(fields)
    }

    pub fn seek(&self, current_handle: usize) -> Option<usize> {
        let offset: usize = self.indices[0].tree.len();
        if current_handle > offset {
            return None;
        }
        match self.indices[0].tree.range((Included(&current_handle), Included(&offset))).next() {
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

        for indexed in self.indices[0].tree.values() {
            indexed.value.print();
        }
    }
}

