pub mod index {
    use std::collections::BTreeMap;
    use std::collections::Bound::Included;

    use indexed::indexed::Indexed;
    use meta::index_info::IndexInfo;
    use tuple::tuple::Tuple;

    pub struct Index<'i> {
        // only adapt BTree index
        // TODO: impl BTree+, Range, Hash index
        pub tree: BTreeMap<usize, Indexed>,
        pub meta: &'i IndexInfo,
    }

    impl<'i> Index<'i> {
        pub fn new(index_info: &'i IndexInfo) -> Index<'i> {
            Index {
                tree: BTreeMap::new(),
                meta: index_info,
            }
        }
        // TODO: IMPL some response as API
        pub fn insert(&mut self, internal_id: usize, tuple: Tuple) {
            if !self.tree.contains_key(&internal_id) {
                let indexed: Indexed = Indexed::new(tuple);
                &mut self.tree.insert(internal_id, indexed);
            }
        }
    }
}

