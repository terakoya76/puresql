pub mod table {
    use std::collections::BTreeMap;
    use std::collections::Bound::Included;
    use std::borrow::ToOwned;

    use field::field::Field;
    use column::column::Column;
    use tuple::tuple::Tuple;
    use item::item::Item;
    use index::index::Index;
    use indexed::indexed::Indexed;
    use meta::table_info::TableInfo;
    use allocator::allocator::Allocator;

    pub struct Table<'t, 'm: 't> {
        pub id: usize,
        pub name: String,
        pub columns: Vec<Column>,
        pub tree: BTreeMap<usize, Item>,
        pub indices: &'t mut Vec<Index<'m>>,
        pub meta: &'t mut TableInfo,
    }

    impl<'t, 'm> Table<'t, 'm> {
        pub fn new(meta: &'t mut TableInfo, indices: &'t mut Vec<Index<'m>>) -> Table<'t, 'm> {
            //Box::new(
                Table {
                    id: meta.id,
                    name: meta.name.clone(),
                    columns: meta.columns.clone(),
                    tree: BTreeMap::new(),
                    indices: indices,
                    meta: meta,
                }
            //)
        }

        // TODO: IMPL some response as API
        pub fn insert(&mut self, fields: Vec<Field>) {
            let internal_id: usize = self.meta.next_record_id.base;
            if !self.tree.contains_key(&internal_id) {
                let tuple: Tuple = Tuple::new(fields);
                let item: Item = Item::new(internal_id, tuple);
                &mut self.tree.insert(internal_id, item);

                for ref mut index in self.indices.iter_mut() {
                    let indexed: Indexed = Indexed::new(internal_id);
                    &mut index.indices.insert(internal_id, indexed);
                }

                &mut self.meta.next_record_id.increament();
            }
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
            let item = self.tree.get(&internal_id);
            if item.is_some() {
                for column in columns {
                    fields.push(item.unwrap().tuple.fields[column.offset].clone());
                }
            }
            Tuple::new(fields)
        }

        pub fn seek(&self, current_handle: usize) -> Option<usize> {
            let offset: usize = self.tree.len();
            if current_handle > offset {
                return None;
            }
            match self.tree.range((Included(&current_handle), Included(&offset))).next() {
                None => self.seek(current_handle+1),
                Some(item) => Some(item.0.clone()),
            }
        }

        pub fn print(&self) {
            let mut col_buffer: String = String::new();
            for col in &self.columns {
                col_buffer += "|";
                col_buffer += &col.name;
            }
            println!("{}", col_buffer);

            for item in self.tree.values() {
                item.tuple.print();
            }
        }
    }
}

