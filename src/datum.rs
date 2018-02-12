pub mod datum {
    #[derive(Clone)]
    pub struct Datum {
        pub value: String,
    }

    impl Datum {
        pub fn new(value: &str) -> Datum {
            Datum {
                value: value.to_string(),
            }
        }
    }
}

