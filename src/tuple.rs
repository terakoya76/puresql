pub mod tuple {
    use datum::datum::Datum;

    #[derive(Clone)]
    pub struct Tuple {
        pub values: Vec<Datum>, 
    }

    impl Tuple {
        pub fn new(values: Vec<Datum>) -> Tuple {
            Tuple {
                values: values,
            }
        }
    }
}

