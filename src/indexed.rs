pub mod indexed {
    use tuple::tuple::Tuple;

    #[derive(Clone)]
    pub struct Indexed {
        // kind: usize - impl each idx: PK, FK, Secondary
        pub value: Tuple,
    }

    impl Indexed {
        pub fn new(value: Tuple) -> Indexed {
            Indexed {
                value: value,
            }
        }
    }
}

