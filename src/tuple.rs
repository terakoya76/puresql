pub mod tuple {
    use field::field::Field;

    #[derive(Clone)]
    pub struct Tuple {
        pub internal_id: u64,
        pub fields: Vec<Field>,
    }

    impl Tuple {
        pub fn new(internal_id: u64, fields: Vec<Field>) -> Tuple {
            Tuple {
                internal_id: internal_id,
                fields: fields,
            }
        }
    }
}

