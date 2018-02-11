use std::string::String;
use std::vec::Vec;
use std::collections::HashMap;
use std::borrow::ToOwned;

pub struct Query {
    columns: Vec<Column>,
    tuples: Vec<Tuple>,
}

impl Query {
    fn new(columns: &Vec<Column>, tuples: &Vec<Tuple>) -> Query {
        Query {
            columns: columns.to_owned(),
            tuples: tuples.to_owned(),
        }
    }

    fn from(tables: &HashMap<String, Table>, table_name: &str) -> Query {
        let opt_table: Option<&Table> = tables.get(table_name);
        if let Some(t) = opt_table {
            let table: Table = t.clone();
            Query::new(&table.columns, &table.tuples)
        } else {
            Query::new(&vec![], &vec![])
        }
    }

    fn select(&self, column_names: Vec<&str>) -> Query {
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
                    values.push(&tuple.values[index]);
                } else {
                    values.push("");
                }
            }

            tuples.push(
                Tuple::new(values.iter().map(|s| s.to_string()).collect())
            );
        }

        Query::new(&columns, &tuples)
    }

    fn left_join(&self, tables: &HashMap<String, Table>, table_name: &str, key_column_name: &str) -> Query {
        let opt_table: Option<&Table> = tables.get(table_name);
        if opt_table.is_none() {
            return Query::new(&self.columns, &self.tuples);
        }
        let table: Table = opt_table.unwrap().clone();

        let l_column_idx: usize = self.find_column(key_column_name);
        let r_column_idx: usize = table.find_column(key_column_name);

        if l_column_idx >= self.columns.len() || r_column_idx >= table.columns.len() {
            return Query::new(&self.columns, &vec![Tuple::new(vec!["".to_string(); self.columns.len()])]);
        }

        let mut new_columns: Vec<Column> = self.columns.iter().map(|c| Column::new(&c.table_name, &c.name)).collect();
        for r_col in &table.columns {
            new_columns.push(Column::new(table_name, &r_col.name));
        }

        let mut new_tuples: Vec<Tuple> = Vec::new();
        for l_tuple in &self.tuples {
            let mut tmp_tuple: Tuple = Tuple::new(l_tuple.values.to_owned());
            while tmp_tuple.values.len() < self.columns.len() {
                tmp_tuple.values.push("".to_string());
            }

            let l_value: String = tmp_tuple.values[l_column_idx].to_string();
            if !l_value.is_empty() {
                for r_tuple in &table.tuples {
                    if r_tuple.values.len() < r_column_idx {
                        continue;
                    }

                    if l_value == r_tuple.values[r_column_idx] {
                        for r_value in &r_tuple.values {
                            tmp_tuple.values.push(r_value.to_string());
                        }
                        break;
                    }
                }
            }

            while tmp_tuple.values.len() < new_columns.len() {
                tmp_tuple.values.push("".to_string());
            }
            new_tuples.push(tmp_tuple);
        }

        Query::new(&new_columns, &new_tuples)
    }

    fn less_than(&self, column_name: &str, value: &str) -> Query {
        let idx: usize = self.find_column(column_name);
        if idx >= self.columns.len() {
            return Query::new(&self.columns, &vec![Tuple::new(vec!["".to_string(); self.columns.len()])]);
        }

        let mut new_tuples: Vec<Tuple> = Vec::new();
        for tuple in &self.tuples {
            if tuple.values[idx] < value.to_string() {
                new_tuples.push(Tuple::new(tuple.values.clone()));
            }
        }

        Query::new(&self.columns, &new_tuples)
    }

    fn find_column(&self, name: &str) -> usize {
        for (i, col) in self.columns.iter().enumerate() {
            if col.name == name {
                return i;
            }
        }

        self.columns.len()
    }

    fn to_string(&self) {
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
                tuple_buffer += &v;
            }
            println!("{}", tuple_buffer);
            tuple_buffer.clear();
        }
    }
}

#[derive(Clone)]
pub struct Table {
    name: String,
    columns: Vec<Column>, 
    tuples: Vec<Tuple>,
}

impl Table {
    fn new(name: &str, columns: &Vec<Column>, tuples: &Vec<Tuple>) -> Table {
        Table {
            name: name.to_string(),
            columns: columns.to_owned(),
            tuples: tuples.to_owned(), 
        }
    }

    fn create(name: &str, column_names: Vec<&str>) -> Table {
        let mut columns: Vec<Column> = Vec::new();
        for c_name in column_names {
            columns.push(Column::new(name, c_name))
        }

        let tuples: Vec<Tuple> = Vec::new();

        Table::new(&name, &columns, &tuples)
    }

    fn find_column(&self, name: &str) -> usize {
        for (i, col) in self.columns.iter().enumerate() {
            if col.name == name {
                return i;
            }
        }
        self.columns.len()
    }

    fn to_string(&self) {
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
                tuple_buffer += &v;
            }
            println!("{}", tuple_buffer);
            tuple_buffer.clear();
        }
    }

    fn insert(&mut self, values: Vec<&str>) {
        &self.tuples.push(
            Tuple::new(values.iter().map(|s| s.to_string()).collect())
        );
    }
} 

#[derive(Clone)]
pub struct Column {
    table_name: String,
    name: String,
}

impl Column {
    fn new(table_name: &str, name: &str) -> Column {
        Column {
            table_name: table_name.to_string(),
            name: name.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct Tuple {
    values: Vec<String>, 
}

impl Tuple {
    fn new(values: Vec<String>) -> Tuple {
        Tuple {
            values: values,
        }
    }
}

fn main() {
    let mut shohin: Table = Table::create("shohin", vec!["shohin_id", "shohin_name", "kubun_id", "price"]);
    shohin.insert(vec!["1", "apple", "1", "300"]);
    shohin.insert(vec!["2", "orange", "1", "130"]);
    shohin.insert(vec!["3", "cabbage", "2", "200"]);
    shohin.insert(vec!["4", "sea weed", "None", "250"]);
    shohin.insert(vec!["5", "mushroom", "3", "100"]);
    shohin.to_string();

    let mut kubun: Table = Table::create("kubun", vec!["kubun_id", "kubun_name"]);
    kubun.insert(vec!["1", "fruit"]);
    kubun.insert(vec!["2", "vegetable"]);

    let mut tables = HashMap::new();
    tables.insert(shohin.clone().name, shohin.clone());
    Query::from(&tables, "shohin").select(vec!["shohin_id", "shohin_name"]).to_string();

    tables.insert(kubun.clone().name, kubun.clone());
    Query::from(&tables, "shohin").left_join(&tables, "kubun", "kubun_id").to_string();
}

