pub mod column {
    #[derive(Clone)]
    pub struct Column {
        pub table_name: String,
        pub name: String,
        pub offset: usize,
    }

    impl Column {
        pub fn new(table_name: &str, name: &str, offset: usize) -> Column {
            Column {
                table_name: table_name.to_string(),
                name: name.to_string(),
                offset: offset,
            }
        }
    }

    pub struct Range {
        pub low: usize,
        pub high: usize,
    }

    impl Range {
        pub fn new(low: usize, high: usize) -> Range {
            Range {
                low: low,
                high: high,
            }
        }
    }
}

