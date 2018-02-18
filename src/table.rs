pub mod table {
    use std::collections::BTreeMap;
    use std::borrow::ToOwned;

    use field::field::Field;
    use column::column::Column;
    use tuple::tuple::Tuple;
    use allocator::allocator::Allocator;

    #[derive(Clone)]
    pub struct Table {
        pub id: u64,
        pub name: String,
        pub columns: Vec<Column>,
        pub alloc: Box<Allocator>,
        pub tree: BTreeMap<u64, Tuple>,
    }

    impl Table {
        pub fn new(id: u64, name: &str, columns: &Vec<Column>, alloc: Box<Allocator>) -> Box<Table> {
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
            let table_id: u64 = alloc.base;
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
            let internal_id: u64 = self.alloc.base;
            if !self.tree.contains_key(&internal_id) {
                let tuple: Tuple = Tuple::new(internal_id, fields);
                &mut self.tree.insert(internal_id, tuple);
                self.alloc.increament();
            }
        }

        pub fn get_fields(&self, columns: Vec<&str>) -> Vec<Field> {
            let filtered_cols: Vec<&Column> = self.columns.iter().filter(|c| columns.contains(&c.name.as_str())).collect();
            let mut fields = Vec::new();
            for internal_id in self.tree.keys() {
                fields.append(&mut self.get_fields_by_columns(internal_id, &filtered_cols));
            }
            fields
        }

        pub fn get_fields_by_columns(&self, internal_id: &u64, columns: &Vec<&Column>) -> Vec<Field> {
            let mut fields = Vec::new();
            let tuple = self.tree.get(&internal_id);
            if tuple.is_some() {
                for column in columns {
                    fields.push(tuple.unwrap().fields[column.offset].clone());
                }
            }
            fields
        }
    }

    pub fn to_string(&self) {
        let mut col_buffer: String = String::new();
        let mut tuple_buffer: String = String::new();

        for col in &self.columns {
            col_buffer += "|";
            col_buffer += &col.name;
        }
        println!("{}", col_buffer);

        for tuple in self.tree.values() {
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
}

