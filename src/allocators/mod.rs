pub mod allocator {
    #[derive(Debug, Clone)]
    pub struct Allocator {
        pub table_id: usize,
        pub base: usize,
    }

    impl Allocator {
        pub fn new(table_id: usize) -> Box<Allocator> {
            Box::new(Allocator {
                table_id: table_id,
                base: 1 as usize,
            })
        }

        pub fn increment(&mut self) {
            self.base += 1;
        }
    }
}
