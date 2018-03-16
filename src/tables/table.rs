use std::collections::Bound::Included;

use columns::column::Column;
use Field;
use Tuple;
use Index;
use Indexed;
use meta::table_info::TableInfo;

// TODO: rm pk_index and generalize impl of index
// index<T> might be usize or RID for indexed data on disk
pub struct Table<'t> {
    pub id: usize,
    pub name: String,
    pub columns: Vec<Column>,
    pub pk_index: Option<Index<Tuple>>,
    pub indices: Vec<Index<usize>>,
    pub meta: &'t mut TableInfo,
}

impl<'t> Table<'t> {
    pub fn new(meta: &'t mut TableInfo) -> Table<'t> {
        let mut columns: Vec<Column> = Vec::new();
        for column_info in &meta.columns {
            columns.push(column_info.to_column(&meta.name));
        }

        let mut pk_index: Option<Index<Tuple>> = None;
        let mut indices: Vec<Index<usize>> = Vec::new();
        for index_info in &meta.indices {
            if index_info.is_pk_index {
                pk_index = Some(Index::new(index_info.clone()));
            } else {
                indices.push(Index::new(index_info.clone()));
            }
        }

        Table {
            id: meta.id,
            name: meta.name.clone(),
            columns: columns,
            pk_index: pk_index,
            indices: indices,
            meta: meta,
        }
    }

    // TODO: IMPL some response as API
    pub fn insert(&mut self, fields: Vec<Field>) {
        let internal_id: usize = self.meta.next_record_id.base;
        let tuple: Tuple = Tuple::new(fields.clone());
        match self.pk_index {
            None => {},
            Some(ref mut idx) => idx.insert(internal_id, tuple),
        }

        //for ref mut index in self.indices.iter_mut() {
        //    &mut index.insert(internal_id, Tuple::new(fields.clone()));
        //}

        &mut self.meta.next_record_id.increament();
    }

    pub fn get_tuple(&self, internal_id: usize) -> Tuple {
        match self.pk_index {
            None => Tuple::new(vec![]),
            Some(ref idx) => {
                match idx.tree.get(&internal_id) {
                    None => Tuple::new(vec![]),
                    Some(indexed) => indexed.value.clone(),
                }
            },
        }
    }

    pub fn seek(&self, current_handle: usize) -> Option<usize> {
        match self.pk_index {
            None => None,
            Some(ref idx) => {
                let offset: usize = idx.tree.len();
                if current_handle > offset {
                    return None;
                }

                match idx.tree.range((Included(&current_handle), Included(&offset))).next() {
                    None => self.seek(current_handle+1),
                    Some(node) => Some(node.0.clone()),
                }
            },
        }
    }

    pub fn print(&self) {
        let mut col_buffer: String = String::new();
        for col in &self.columns {
            col_buffer += "|";
            col_buffer += &col.name;
        }
        println!("{}", col_buffer);

        match self.pk_index {
            None => {},
            Some(ref idx) => {
                for indexed in idx.tree.values() {
                    indexed.value.print();
                }
            },
        }
    }
}

