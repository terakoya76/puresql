// trait
use ScanExec;

// struct
use columns::column::Column;
use columns::range::Range;
use tables::tuple::Tuple;
use tables::table::Table;

pub struct TableScanExec<'ts, 't: 'ts> {
    pub table: &'ts Table<'t>,
    pub name: String,
    pub ranges: Vec<Range>,
    pub cursor: usize,
    pub seek_handle: usize,
    pub columns: Vec<Column>,
}

impl<'ts, 't> TableScanExec<'ts, 't> {
    pub fn new(table: &'ts Table<'t>, name: &str, ranges: Vec<Range>) -> TableScanExec<'ts, 't> {
        TableScanExec {
            table: table,
            name: name.to_string(),
            ranges: ranges,
            cursor: 0,
            seek_handle: 0,
            columns: table.columns.iter().map(|c| c.clone()).collect(),
        }
    }
}

impl<'ts, 't> ScanExec for TableScanExec<'ts, 't> {
    fn get_columns(&self) -> Vec<Column> {
        self.columns.clone()
    }

    fn get_tuple(&self, handle: usize) -> Tuple {
        self.table.get_tuple(handle)
    }

    fn set_next_handle(&mut self, next_handle: usize) {
        self.seek_handle = next_handle;
    }

    fn next_handle(&mut self) -> Option<usize> {
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
}

impl<'ts, 't> Iterator for TableScanExec<'ts, 't> {
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
