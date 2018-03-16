use std::collections::BTreeMap;
use std::collections::Bound::Included;

use tables::tuple::Tuple;
use tables::indexed::Indexed;
use meta::index_info::IndexInfo;

pub struct Index<T> {
    // only adapt BTree index
    // TODO: impl BTree+, Range, Hash index
    pub tree: BTreeMap<usize, Indexed<T>>,
    pub meta: IndexInfo,
}

impl<T> Index<T> {
    pub fn new(index_info: IndexInfo) -> Index<T> {
        Index {
            tree: BTreeMap::new(),
            meta: index_info,
        }
    }

    // TODO: IMPL some response as API
    pub fn insert(&mut self, internal_id: usize, value: T) {
        if !self.tree.contains_key(&internal_id) {
            let indexed: Indexed<T> = Indexed::new(value);
            &mut self.tree.insert(internal_id, indexed);
        }
    }
}
