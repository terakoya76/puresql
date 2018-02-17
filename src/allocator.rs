pub mod allocator {
    #[derive(Clone)]
    pub struct Allocator {
        pub db_id: usize,
        pub base: u64,
    }

    impl Allocator {
        pub fn new(db_id: usize) -> Allocator {
            Allocator {
                db_id: db_id,
                base: 1 as u64,
            }
        }

        pub fn increament(&mut self) {
            self.base += 1;
        }
    }
}

