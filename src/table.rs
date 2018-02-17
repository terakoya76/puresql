pub mod table {
    use std::collections::BTreeMap;

    use field::field::Field;
    use column::column::Column;
    use tuple::tuple::Tuple;
    use allocator::allocator::Allocator;

    #[derive(Clone)]
    pub struct Table {
        pub id: usize,
        pub name: String,
        pub columns: Vec<Column>,
        pub alloc: Box<Allocator>,
        pub tree: BTreeMap<u64, Tuple>,
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

        pub fn create(id: usize, name: &str, column_names: Vec<&str>, alloc: Box<Allocator>) -> Box<Table> {
            let mut columns: Vec<Column> = Vec::new();
            for c_name in column_names {
                columns.push(Column::new(name, c_name))
            }
            Table::new(id, &name, &columns, alloc)
        }

        /*
        pub fn from(&self) -> Relation {
            Relation::new(&self.columns, &self.tuples)
        }
        */

        /*
        pub fn find_column(&self, column_name: &str) -> usize {
            for (i, col) in self.columns.iter().enumerate() {
                if col.name == column_name {
                    return i;
                }
            }
            self.columns.len()
        }
        */

        pub fn to_string(&self) {
            let mut col_buffer: String = String::new();
            let mut tuple_buffer: String = String::new();

            for col in &self.columns {
                col_buffer += "|";
                col_buffer += &col.name;
            }
            println!("{}", col_buffer);

            for (internal_id, tuple) in self.tree.iter() {
                for f in &tuple.fields {
                    tuple_buffer += "|";
                    match f.kind {
                        ::field::field::KIND_I64 => tuple_buffer += &f.get_i64().to_string(),
                        ::field::field::KIND_U64 => tuple_buffer += &f.get_u64().to_string(),
                        ::field::field::KIND_F64 => tuple_buffer += &f.get_f64().to_string(),
                        ::field::field::KIND_STR => tuple_buffer += &f.get_str(),
                        _ => tuple_buffer += "Unsupported Data Type",
                    }
                }
                println!("{}", tuple_buffer);
                tuple_buffer.clear();
            }
        }

        // TODO: IMPL some response as API
        pub fn insert(&mut self, tuple: Tuple) {
            let internal_id: u64 = self.alloc.base;
            if !self.tree.contains_key(&internal_id) {
                &mut self.tree.insert(internal_id, tuple);
                self.alloc.increament();
            }
        }

        /*
        pub fn tuple_with_columns(&self, internal_id: usize, columns: Vec<Column>) -> Vec<Field> {
            let mut v = Vec::new();
            let tuple = self.tree.get(&internal_id);
            match tuple {
                Some(&Tuple) => {
                    for column in columns {
                        v.push(tuple.unwrap().fields[column.pos]);
                    }
                }
            }
            v
        }
        */
    }
}

