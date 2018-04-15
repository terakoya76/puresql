use ScanIterator;
use columns::column::Column;
use columns::range::Range;
use tables::tuple::Tuple;
use tables::memory_table::MemoryTable;

#[derive(Debug)]
pub struct MemoryTableScanExec<'t> {
    pub table: &'t mut MemoryTable,
    pub ranges: Vec<Range>,
    pub cursor: usize,
    pub seek_handle: usize,
    pub columns: Vec<Column>,
}

impl<'t> MemoryTableScanExec<'t> {
    pub fn new(table: &'t mut MemoryTable, ranges: Vec<Range>) -> MemoryTableScanExec<'t> {
        let columns: Vec<Column> = table.columns.iter().map(|c| c.clone()).collect();
        MemoryTableScanExec {
            table: table,
            ranges: ranges,
            cursor: 0,
            seek_handle: 0,
            columns: columns,
        }
    }

    fn get_tuple(&mut self, handle: usize) -> Tuple {
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

impl<'t> ScanIterator for MemoryTableScanExec<'t> {
    fn get_columns(&self) -> Vec<Column> {
        self.columns.clone()
    }
}

impl<'t> Iterator for MemoryTableScanExec<'t> {
    type Item = Tuple;
    fn next(&mut self) -> Option<Tuple> {
        match self.next_handle() {
            None => None,
            Some(handle) => {
                &mut self.set_next_handle(handle + 1);
                Some(self.get_tuple(handle))
            },
        }
    }
}

