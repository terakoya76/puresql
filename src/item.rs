pub mod item {
    use tuple::tuple::Tuple;

    #[derive(Clone)]
    pub struct Item {
        pub internal_id: usize,
        pub tuple: Tuple,
    }

    impl Item {
        pub fn new(internal_id: usize, tuple: Tuple) -> Item {
            Item {
                internal_id: internal_id,
                tuple: tuple,
            }
        }

        pub fn print(&self) {
            self.tuple.print();
        }
    }
}

