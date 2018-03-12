pub mod indexed {

    #[derive(Clone)]
    pub struct Indexed {
        // kind: usize - impl each idx: PK, FK, Secondary
        value: usize,
    }

    impl Indexed {
        pub fn new(handle: usize) -> Indexed {
            Indexed {
                value: handle,
            }
        }
    }
}

