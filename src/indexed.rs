pub mod indexed {
    use tuple::tuple::Tuple;

    #[derive(Clone)]
    pub struct Indexed<T> {
        // kind: usize - impl each idx: PK, FK, Secondary
        pub value: T,
    }

    impl<T> Indexed<T> {
        pub fn new(value: T) -> Indexed<T> {
            Indexed {
                value: value,
            }
        }
    }
}

