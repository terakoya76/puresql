use std::string:String
use std::vec::Vec
use std::collections::HashMap;
use std::any::Any;

pub mod app {
	let mut tables: HashMap<String, Table> = HashMap::new();

	pub struct Table {
		name: String,
		columns: Vec<Column>,
        taples: Vec<Taple>,
	}

	impl Table {
		fn new(name: &str, columns: Vec<Column>) -> Table {
			Table {
				name: name,
				columns: columns,
			}
		}

        fn create(name: &str, column_names: &[String]) -> Table {
			let mut columns: Vec<Column> = Vec::new();

			for c_name in column_names {
				columns.push(Column::new(name, c_name));
			}

			let table: Table = Table::new(name, columns);

			// tablesにTableインスタンスを追加して管理するところは要修正
			tables.push(name, table);

			return table;
		}
	} 

	struct Column {
		table: String,
		name: String,
	}

	impl Column {
		fn new(table: &str, name: &str) {
			Column {
				table: table,
				name: name,
			}
		}
	}

    pub struct Tuple {
        values: Vec<Any>,
    }
}





fn main() {
    println!("Hello, world!");
}

