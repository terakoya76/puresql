pub mod table {    
    use datum::datum::Datum;
    use column::column::Column;
    use tuple::tuple::Tuple;
    use relation::relation::Relation;

    #[derive(Clone)]
    pub struct Table {
        pub name: String,
        pub columns: Vec<Column>, 
        pub tuples: Vec<Tuple>,
    }

    impl Table {
        pub fn new(name: &str, columns: &Vec<Column>, tuples: &Vec<Tuple>) -> Table {
            Table {
                name: name.to_string(),
                columns: columns.to_owned(),
                tuples: tuples.to_owned(), 
            }
        }

        pub fn create(name: &str, column_names: Vec<&str>) -> Table {
            let mut columns: Vec<Column> = Vec::new();
            for c_name in column_names {
                columns.push(Column::new(name, c_name))
            }

            let tuples: Vec<Tuple> = Vec::new();

            Table::new(&name, &columns, &tuples)
        }

        pub fn from(&self) -> Relation {
            Relation::new(&self.columns, &self.tuples)
        }

        pub fn find_column(&self, column_name: &str) -> usize {
            for (i, col) in self.columns.iter().enumerate() {
                if col.name == column_name {
                    return i;
                }
            }
            self.columns.len()
        }

        pub fn to_string(&self) {
            let mut col_buffer: String = String::new();
            let mut tuple_buffer: String = String::new();

            for col in &self.columns {
                col_buffer += "|";
                col_buffer += &col.name;
            }
            println!("{}", col_buffer);

            for tuple in &self.tuples {
                for v in &tuple.values {
                    tuple_buffer += "|";
                    tuple_buffer += &v.value;
                }
                println!("{}", tuple_buffer);
                tuple_buffer.clear();
            }
        }

        pub fn insert(&mut self, values: Vec<&str>) {
            &self.tuples.push(
                Tuple::new(values.iter().map(|s| Datum::new(s)).collect())
            );
        }
    } 
}

