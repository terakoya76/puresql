pub mod relation {
    use datum::datum::Datum;
    use column::column::Column;
    use tuple::tuple::Tuple;

    pub struct Relation {
        pub columns: Vec<Column>,
        pub tuples: Vec<Tuple>,
    }

    impl Relation {
        pub fn new(columns: &Vec<Column>, tuples: &Vec<Tuple>) -> Relation {
            Relation {
                columns: columns.to_owned(),
                tuples: tuples.to_owned(),
            }
        }

        pub fn select(&self, column_names: Vec<&str>) -> Relation {
            let mut columns: Vec<Column> = Vec::new();
            let mut tuples: Vec<Tuple> = Vec::new();
            let mut indexes: Vec<usize> = Vec::new();

            for col_name in column_names {
                let index: usize = self.find_column(col_name);
                let table_name: &str = &self.columns[index].table_name;
                columns.push(Column::new(table_name, col_name));
                indexes.push(index);
            }

            for tuple in &self.tuples {
                let mut values: Vec<&str> = Vec::new();
                for index in indexes.iter().cloned() {
                    if index < tuple.values.len() {
                        values.push(&tuple.values[index].value);
                    } else {
                        values.push("");
                    }
                }

                tuples.push(
                    Tuple::new(values.iter().map(|s| Datum::new(s)).collect())
                );
            }

            Relation::new(&columns, &tuples)
        }

        pub fn find_column(&self, column_name: &str) -> usize {
            for (i, col) in self.columns.iter().enumerate() {
                if col.name == column_name {
                    return i;
                }
            }
            self.columns.len()
        }

        pub fn left_join(&self, relation: Relation, key_column: &str) -> Relation {
            let l_column_idx: usize = self.find_column(key_column);
            let r_column_idx: usize = relation.find_column(key_column);

            if l_column_idx >= self.columns.len() || r_column_idx >= relation.columns.len() {
                return Relation::new(&self.columns, &vec![Tuple::new(vec![Datum::new(""); self.columns.len()])]);
            }

            let mut new_columns: Vec<Column> = self.columns.iter().map(|c| Column::new(&c.table_name, &c.name)).collect();
            for r_col in &relation.columns {
                new_columns.push(Column::new(&r_col.table_name, &r_col.name));
            }

            let mut new_tuples: Vec<Tuple> = Vec::new();
            for l_tuple in &self.tuples {
                let mut tmp_tuple: Tuple = Tuple::new(l_tuple.values.to_owned());
                while tmp_tuple.values.len() < self.columns.len() {
                    tmp_tuple.values.push(Datum::new(""));
                }

                let l_value: String = tmp_tuple.values[l_column_idx].value.to_string();
                if !l_value.is_empty() {
                    for r_tuple in &relation.tuples {
                        if r_tuple.values.len() < r_column_idx {
                            continue;
                        }

                        if l_value == r_tuple.values[r_column_idx].value {
                            for r_value in &r_tuple.values {
                                tmp_tuple.values.push(Datum::new(&r_value.value));
                            }
                            break;
                        }
                    }
                }

                while tmp_tuple.values.len() < new_columns.len() {
                    tmp_tuple.values.push(Datum::new(""));
                }
                new_tuples.push(tmp_tuple);
            }

            Relation::new(&new_columns, &new_tuples)
        }

        pub fn equal(&self, key_column: &str, value: &str) -> Relation {
            let index: usize = self.find_column(key_column);
            if index >= self.columns.len() {
                return Relation::new(&self.columns, &vec![Tuple::new(vec![Datum::new(""); self.columns.len()])]);
            }

            let mut new_tuples: Vec<Tuple> = Vec::new();
            for tuple in &self.tuples {
                if value == tuple.values[index].value {
                    new_tuples.push(Tuple::new(tuple.values.clone()));
                }
            }
            Relation::new(&self.columns, &new_tuples)
        }

        pub fn less_than(&self, key_column: &str, value: &str) -> Relation {
            let idx: usize = self.find_column(key_column);
            if idx >= self.columns.len() {
                return Relation::new(&self.columns, &vec![Tuple::new(vec![Datum::new(""); self.columns.len()])]);
            }

            let mut new_tuples: Vec<Tuple> = Vec::new();
            for tuple in &self.tuples {
                if tuple.values[idx].value < value.to_string() {
                    new_tuples.push(Tuple::new(tuple.values.clone()));
                }
            }

            Relation::new(&self.columns, &new_tuples)
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
    }
}

