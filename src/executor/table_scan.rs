use column::column::Column;
use column::range::Range;
use tuple::tuple::Tuple;
use table::table::Table;

pub struct TableScanExec<'a> {
    pub table: &'a Box<Table>,
    pub name: String,
    pub ranges: Vec<Range>,
    pub cursor: usize,
    pub seek_handle: usize,
    pub columns: Vec<Column>,
}

impl<'a> TableScanExec<'a> {
    pub fn new(table: &'a Box<Table>, name: &str, ranges: Vec<Range>) -> TableScanExec<'a> {
        TableScanExec {
            table: &table,
            name: name.to_string(),
            ranges: ranges,
            cursor: 0,
            seek_handle: 0,
            columns: table.columns.iter().map(|c| c.clone()).collect(),
        }
    }

    pub fn next_handle(&mut self) -> Option<usize> {
        loop {
            if self.cursor >= self.ranges.len() {
                return None;
            }

            let range: &Range = &self.ranges[self.cursor];
            if self.seek_handle < range.low {
                *&mut self.seek_handle = range.low.clone();
            }

            if self.seek_handle > range.high {
                *&mut self.cursor += 1;
                continue;
            }

            match self.table.seek(self.seek_handle) {
                None => {
                    *&mut self.seek_handle = 0;
                    return None;
                },
                Some(handle) => return Some(handle),
            }
        }
    }

    pub fn get_tuple(&self, handle: usize) -> Tuple {
        self.table.get_fields_by_columns(handle, &self.columns)
    }
}

impl<'a> Iterator for TableScanExec<'a> {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        match self.next_handle() {
            None => None,
            Some(handle) => {
                *&mut self.seek_handle = handle + 1;
                Some(self.get_tuple(handle))
            },
        }
    }
}

