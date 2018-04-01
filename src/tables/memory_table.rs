use std::collections::BTreeMap;
use std::collections::Bound::Included;

use storage::b_tree::BTree;
use columns::column::Column;
use Field;
use Tuple;
use meta::table_info::TableInfo;

#[derive(Debug)]
pub struct MemoryTable<'t> {
    pub id: usize,
    pub name: String,
    pub columns: Vec<Column>,
    pub tree: BTree,
    pub meta: &'t mut TableInfo,
}

impl<'t> MemoryTable<'t> {
    pub fn new(meta: &'t mut TableInfo) -> Result<MemoryTable<'t>, ()> {
        let mut columns: Vec<Column> = Vec::new();
        for column_info in &meta.columns {
            columns.push(column_info.to_column(&meta.name));
        }

        let file_path: String = meta.get_bin_path();
        let btree: BTree = match BTree::new(&file_path) {
            Ok(btree) => btree,
            _ => return Err(()),
        };

        Ok(MemoryTable {
            id: meta.id,
            name: meta.name.clone(),
            columns: columns,
            tree: btree,
            meta: meta,
        })
    }

    // TODO: IMPL some response as API
    pub fn insert(&mut self, fields: Vec<Field>) {
        let record_id = self.meta.next_record_id.base;
        let tuple: Tuple = Tuple::new(fields);
        let encoded: Vec<u8> = match tuple.encode() {
            Ok(bytes) => bytes,
            _ => return (),
        };
        &mut self.tree.insert(record_id, &encoded);
        &mut self.meta.next_record_id.increment();
    }

    pub fn get_tuple(&mut self, internal_id: usize) -> Tuple {
        let mut buf: Vec<u8> = self.tree.get_record(internal_id);
        match Tuple::decode(&mut buf) {
            Ok(tuple) => tuple,
            Err(e) => {
                println!("{:?}", e);
                Tuple::new(vec![])
            },
        }
    }

    pub fn seek(&self, current_handle: usize) -> Option<usize> {
        let offset: usize = self.tree.tree.len();
        if current_handle > offset {
            return None;
        }

        match self.tree.tree.range((Included(&current_handle), Included(&offset))).next() {
            None => self.seek(current_handle+1),
            Some(node) => Some(node.0.clone()),
        }
    }
}

