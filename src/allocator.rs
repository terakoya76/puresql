pub mod allocator {
    #[derive(Clone)]
    pub struct Allocator {
        pub table_id: u64,
        pub base: u64,
    }

    impl Allocator {
        pub fn new(table_id: u64) -> Box<Allocator> {
            Box::new(
                Allocator {
                    table_id: table_id,
                    base: 1 as u64,
                }
            )
        }

        pub fn increament(&mut self) {
            self.base += 1;
        }
    }
}

