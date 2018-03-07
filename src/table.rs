pub mod table {
    use std::collections::BTreeMap;
    use std::collections::Bound::Included;
    use std::borrow::ToOwned;

    use field::field::Field;
    use column::column::Column;
    use tuple::tuple::Tuple;
    use item::item::Item;
    use allocator::allocator::Allocator;

    #[derive(Clone)]
    pub struct Table {
        pub id: usize,
        pub name: String,
        pub columns: Vec<Column>,
        pub alloc: Box<Allocator>,
        pub tree: BTreeMap<usize, Item>,
    }

    impl Table {
        pub fn new(id: usize, name: &str, columns: &Vec<Column>, alloc: Box<Allocator>) -> Box<Table> {
            Box::new(
                Table {
                    id: id,
                    name: name.to_string(),
                    columns: columns.to_owned(),
                    alloc: alloc,
                    tree: BTreeMap::new(),
                }
            )
        }

        pub fn create(alloc: &mut Box<Allocator>, name: &str, column_names: Vec<&str>) -> Box<Table> {
            let table_id: usize = alloc.base;
            alloc.increament();

            let mut columns: Vec<Column> = Vec::new();
            for (i, c_name) in column_names.iter().enumerate() {
                columns.push(Column::new(name, c_name, i))
            }

            let alloc: Box<Allocator> = Allocator::new(table_id);
            Table::new(table_id, &name, &columns, alloc)
        }

        // TODO: IMPL some response as API
        pub fn insert(&mut self, fields: Vec<Field>) {
            let internal_id: usize = self.alloc.base;
            if !self.tree.contains_key(&internal_id) {
                let tuple: Tuple = Tuple::new(fields);
                let item: Item = Item::new(internal_id, tuple);
                &mut self.tree.insert(internal_id, item);
                self.alloc.increament();
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

