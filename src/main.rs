mod field;
mod column;
mod tuple;
mod tables;
mod index;
mod indexed;
mod meta;
mod allocator;
mod executor;

// trait
pub use executor::scan_exec::ScanExec;

// struct
pub use field::field::Field;
pub use column::column::Column;
pub use column::range::Range;
pub use tuple::tuple::Tuple;
pub use tables::table::Table;
pub use tables::memory_table::MemoryTable;
pub use index::index::Index;
pub use indexed::indexed::Indexed;
pub use meta::table_info::TableInfo;
pub use meta::column_info::ColumnInfo;
pub use meta::index_info::IndexInfo;
pub use allocator::allocator::Allocator;
pub use executor::table_scan::TableScanExec;
pub use executor::memory_table_scan::MemoryTableScanExec;
pub use executor::join::NestedLoopJoinExec;
pub use executor::selection::SelectionExec;
pub use executor::selector::{equal, lt, le, gt, ge};
pub use executor::projection::ProjectionExec;
pub use executor::aggregation::AggregationExec;
pub use executor::aggregator::{Aggregator, AggrCount, AggrSum, AggrAvg};

fn main() {
    println!("Table on memory");
    let mut alloc: Box<Allocator> = Allocator::new(1);

    let mut shohin_info: TableInfo = TableInfo::new(&mut alloc, "shohin", vec!["shohin_id", "shohin_name", "kubun_id", "price"], vec![/* IndexInfo */]);
    let mut m_shohin: MemoryTable = MemoryTable::new(&mut shohin_info);
    m_shohin.insert(vec![Field::set_u64(1), Field::set_str("apple"), Field::set_u64(1), Field::set_u64(300)]);
    m_shohin.insert(vec![Field::set_u64(2), Field::set_str("orange"), Field::set_u64(1), Field::set_u64(130)]);
    m_shohin.insert(vec![Field::set_u64(3), Field::set_str("cabbage"), Field::set_u64(2), Field::set_u64(200)]);
    m_shohin.insert(vec![Field::set_u64(4), Field::set_str("sea weed"), Field::set_u64(5), Field::set_u64(250)]);
    m_shohin.insert(vec![Field::set_u64(5), Field::set_str("mushroom"), Field::set_u64(3), Field::set_u64(100)]);
    m_shohin.print();
    println!("");

    let mut kubun_info: TableInfo = TableInfo::new(&mut alloc, "kubun", vec!["kubun_id", "kubun_name"], vec![]);
    let mut m_kubun: MemoryTable = MemoryTable::new(&mut kubun_info);
    m_kubun.insert(vec![Field::set_u64(1), Field::set_str("fruit")]);
    m_kubun.insert(vec![Field::set_u64(2), Field::set_str("vegetable")]);
    m_kubun.print();
    println!("");

    println!("table scan");
    let mut m_shohin_tb_scan: MemoryTableScanExec = MemoryTableScanExec::new(&m_shohin, &m_shohin.name, vec![Range::new(0, 10)]);

    {
        loop {
            match m_shohin_tb_scan.next() {
                None => break,
                Some(tuple) => tuple.print(),
            }
        }
        println!("Scaned\n");
    }

    println!("joined table scan");
    let mut m_kubun_tb_scan: MemoryTableScanExec = MemoryTableScanExec::new(&m_kubun, &m_kubun.name, vec![Range::new(0, 10)]);

    {
        let mut join_exec: NestedLoopJoinExec<MemoryTableScanExec, MemoryTableScanExec> = NestedLoopJoinExec::new(&mut m_shohin_tb_scan, &mut m_kubun_tb_scan);
        loop {
            match join_exec.next() {
                None => break,
                Some(tuple) => tuple.print(),
            }
        }
        println!("Scaned\n");
    }

    println!("selection");
    {
        let mut selection: SelectionExec<MemoryTableScanExec> = SelectionExec::new(&mut m_shohin_tb_scan, vec![equal("shohin_name", Field::set_str("apple"))]);
        loop {
            match selection.next() {
                None => break,
                Some(tuple) => tuple.print(),
            }
        }
        println!("Scaned\n");
    }

    {
        let mut selection: SelectionExec<MemoryTableScanExec> = SelectionExec::new(&mut m_shohin_tb_scan, vec![le("shohin_id", Field::set_u64(3))]);
        loop {
            match selection.next() {
                None => break,
                Some(tuple) => tuple.print(),
            }
        }
        println!("Scaned\n");
    }

    println!("projection");
    {
        let mut projection: ProjectionExec<MemoryTableScanExec> = ProjectionExec::new(&mut m_shohin_tb_scan, vec!["shohin_name", "price"]);
        loop {
            match projection.next() {
                None => break,
                Some(tuple) => tuple.print(),
            }
        }
        println!("Scaned\n");
    }

    println!("aggregation\n");
    {
        let mut aggregation = AggregationExec::new(&mut m_shohin_tb_scan, vec![], vec![AggrCount::new(), AggrSum::new("price"), AggrAvg::new("price")]);
        loop {
            match aggregation.next() {
                None => break,
                Some(tuples) => {
                    for tuple in tuples {
                        tuple.print();
                    }
                },
            }
        }
        println!("Scaned\n");
    }

    println!("group by aggregation\n");
    {
        let mut grouped = AggregationExec::new(&mut m_shohin_tb_scan, vec!["price"], vec![AggrCount::new(), AggrSum::new("price"), AggrAvg::new("price")]);
        loop {
            match grouped.next() {
                None => break,
                Some(tuples) => {
                    for tuple in tuples {
                        tuple.print();
                    }
                },
            }
            println!("");
        }
        println!("Scaned\n");
    }

    println!("Table with index");
    let mut shohin_info: TableInfo = TableInfo::new(&mut alloc, "shohin", vec!["shohin_id", "shohin_name", "kubun_id", "price"], vec![/* IndexInfo */]);
    let shohin_id_index_info: IndexInfo = IndexInfo::new(&mut shohin_info, "shohin_id", vec!["shohin_id"]);

    let mut shohin_id_index: Index = Index::new(&shohin_id_index_info);
    let shohin_idx: Vec<&mut Index> = vec![&mut shohin_id_index];

    let mut shohin: Table = Table::new(&mut shohin_info, shohin_idx);
    shohin.insert(vec![Field::set_u64(1), Field::set_str("apple"), Field::set_u64(1), Field::set_u64(300)]);
    shohin.insert(vec![Field::set_u64(2), Field::set_str("orange"), Field::set_u64(1), Field::set_u64(130)]);
    shohin.insert(vec![Field::set_u64(3), Field::set_str("cabbage"), Field::set_u64(2), Field::set_u64(200)]);
    shohin.insert(vec![Field::set_u64(4), Field::set_str("sea weed"), Field::set_u64(5), Field::set_u64(250)]);
    shohin.insert(vec![Field::set_u64(5), Field::set_str("mushroom"), Field::set_u64(3), Field::set_u64(100)]);
    shohin.print();
    println!("");

    let mut kubun_info: TableInfo = TableInfo::new(&mut alloc, "kubun", vec!["kubun_id", "kubun_name"], vec![/* IndexInfo */]);
    let kubun_id_index_info: IndexInfo = IndexInfo::new(&mut kubun_info, "kubun_id", vec!["kubun_id"]);

    let mut kubun_id_index: Index = Index::new(&kubun_id_index_info);
    let kubun_idx: Vec<&mut Index> = vec![&mut kubun_id_index];

    let mut kubun: Table = Table::new(&mut kubun_info, kubun_idx);
    kubun.insert(vec![Field::set_u64(1), Field::set_str("fruit")]);
    kubun.insert(vec![Field::set_u64(2), Field::set_str("vegetable")]);
    kubun.print();
    println!("");

    println!("table scan");
    let mut shohin_tb_scan: TableScanExec = TableScanExec::new(&shohin, &shohin.name, vec![Range::new(0, 10)]);

    {
        loop {
            match shohin_tb_scan.next() {
                None => break,
                Some(tuple) => tuple.print(),
            }
        }
        println!("Scaned\n");
    }

    println!("joined table scan");
    let mut kubun_tb_scan: TableScanExec = TableScanExec::new(&kubun, &kubun.name, vec![Range::new(0, 10)]);

    {
        let mut join_exec: NestedLoopJoinExec<TableScanExec, TableScanExec> = NestedLoopJoinExec::new(&mut shohin_tb_scan, &mut kubun_tb_scan);
        loop {
            match join_exec.next() {
                None => break,
                Some(tuple) => tuple.print(),
            }
        }
        println!("Scaned\n");
    }

    println!("selection");
    {
        let mut selection: SelectionExec<TableScanExec> = SelectionExec::new(&mut shohin_tb_scan, vec![equal("shohin_name", Field::set_str("apple"))]);
        loop {
            match selection.next() {
                None => break,
                Some(tuple) => tuple.print(),
            }
        }
        println!("Scaned\n");
    }

    {
        let mut selection: SelectionExec<TableScanExec> = SelectionExec::new(&mut shohin_tb_scan, vec![le("shohin_id", Field::set_u64(3))]);
        loop {
            match selection.next() {
                None => break,
                Some(tuple) => tuple.print(),
            }
        }
        println!("Scaned\n");
    }

    println!("projection");
    {
        let mut projection: ProjectionExec<TableScanExec> = ProjectionExec::new(&mut shohin_tb_scan, vec!["shohin_name", "price"]);
        loop {
            match projection.next() {
                None => break,
                Some(tuple) => tuple.print(),
            }
        }
        println!("Scaned\n");
    }

    println!("aggregation\n");
    {
        let mut aggregation = AggregationExec::new(&mut shohin_tb_scan, vec![], vec![AggrCount::new(), AggrSum::new("price"), AggrAvg::new("price")]);
        loop {
            match aggregation.next() {
                None => break,
                Some(tuples) => {
                    for tuple in tuples {
                        tuple.print();
                    }
                },
            }
        }
        println!("Scaned\n");
    }

    println!("group by aggregation\n");
    {
        let mut grouped = AggregationExec::new(&mut shohin_tb_scan, vec!["price"], vec![AggrCount::new(), AggrSum::new("price"), AggrAvg::new("price")]);
        loop {
            match grouped.next() {
                None => break,
                Some(tuples) => {
                    for tuple in tuples {
                        tuple.print();
                    }
                },
            }
            println!("");
        }
        println!("Scaned\n");
    }
}

