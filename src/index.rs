pub mod index {
    use std::collections::BTreeMap;
    use std::collections::Bound::Included;

    use indexed::indexed::Indexed;
    use meta::index_info::IndexInfo;

    pub struct Index<'i> {
        // only adapt BTree index
        // TODO: impl BTree+, Range, Hash index
        pub indices: BTreeMap<usize, Indexed>,
        pub meta: &'i IndexInfo,
    }

    impl<'i> Index<'i> {
        pub fn new(index_info: &'i IndexInfo) -> Index<'i> {
            Index {
                indices: BTreeMap::new(),
                meta: index_info,
            }
        }

        pub fn insert(&mut self, handle: usize, data: Indexed) {
            if !self.indices.contains_key(&handle) {
                &mut self.indices.insert(handle, data);
            }
        }
    }
}

