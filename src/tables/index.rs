use std::collections::BTreeMap;

use meta::index_info::IndexInfo;

#[derive(Debug)]
pub struct Index<T> {
    // only adapt BTree index
    // TODO: impl BTree+, Range, Hash index
    pub tree: BTreeMap<usize, T>,
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
            &mut self.tree.insert(internal_id, value);
        }
    }
}

#[cfg(test)]
mod tests {}
