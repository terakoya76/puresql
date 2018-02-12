pub mod column {
    #[derive(Clone)]
    pub struct Column {
        pub table_name: String,
        pub name: String,
    }

    impl Column {
        pub fn new(table_name: &str, name: &str) -> Column {
            Column {
                table_name: table_name.to_string(),
                name: name.to_string(),
            }
        }
    }
}

