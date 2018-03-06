mod field;
mod column;
mod tuple;
mod item;
mod table;
mod allocator;
mod executor;

pub use field::field::Field;
pub use column::column::Column;
pub use column::range::Range;
pub use tuple::tuple::Tuple;
pub use item::item::Item;
pub use table::table::Table;
pub use allocator::allocator::Allocator;
pub use executor::table_scan::TableScanExec;
pub use executor::join::NestedLoopJoinExec;
pub use executor::selection::SelectionExec;
pub use executor::selector::{equal, lt, le, gt, ge};
pub use executor::projection::ProjectionExec;
pub use executor::aggregation::AggregationExec;
pub use executor::aggregator::{Aggregator, AggrCount, AggrSum, AggrAvg};

fn main() {
    println!("Whole Table");
    let mut alloc: Box<Allocator> = Allocator::new(1);
    let mut shohin: Box<Table> = Table::create(&mut alloc, "shohin", vec!["shohin_id", "shohin_name", "kubun_id", "price"]);
    shohin.insert(vec![Field::set_u64(1), Field::set_str("apple"), Field::set_u64(1), Field::set_u64(300)]);
    shohin.insert(vec![Field::set_u64(2), Field::set_str("orange"), Field::set_u64(1), Field::set_u64(130)]);
    shohin.insert(vec![Field::set_u64(3), Field::set_str("cabbage"), Field::set_u64(2), Field::set_u64(200)]);
    shohin.insert(vec![Field::set_u64(4), Field::set_str("sea weed"), Field::set_u64(5), Field::set_u64(250)]);
    shohin.insert(vec![Field::set_u64(5), Field::set_str("mushroom"), Field::set_u64(3), Field::set_u64(100)]);
    shohin.to_string();
    println!("");

    let mut kubun: Box<Table> = Table::create(&mut alloc, "kubun", vec!["kubun_id", "kubun_name"]);
    kubun.insert(vec![Field::set_u64(1), Field::set_str("fruit")]);
    kubun.insert(vec![Field::set_u64(2), Field::set_str("vegetable")]);
    kubun.to_string();
    println!("");

    println!("table scan");
    let mut shohin_tb_scan: TableScanExec = TableScanExec::new(&shohin, &shohin.name, vec![Range::new(0, 10)]);

    {
        loop {
            match shohin_tb_scan.next() {
                None => break,
                Some(tuple) => tuple.to_string(),
            }
        }
        println!("Scaned\n");
    }

    println!("joined table scan");
    let mut kubun_tb_scan: TableScanExec = TableScanExec::new(&kubun, &kubun.name, vec![Range::new(0, 10)]);

    {
        let mut joined_exec: NestedLoopJoinExec = NestedLoopJoinExec::new(&mut shohin_tb_scan, &mut kubun_tb_scan);
        let joined_tps: Vec<Tuple> = joined_exec.join();
        for tp in joined_tps.iter() {
            tp.to_string();
        }
        println!("Scaned\n");
    }

    println!("selection");

    {
        let mut selection: SelectionExec = SelectionExec::new(&mut shohin_tb_scan, vec![equal("shohin_name", Field::set_str("apple"))]);
        loop {
            match selection.next() {
                None => break,
                Some(tuple) => tuple.to_string(),
            }
        }
        println!("Scaned\n");
    }

    {
        let mut selection: SelectionExec = SelectionExec::new(&mut shohin_tb_scan, vec![le("shohin_id", Field::set_u64(3))]);
        loop {
            match selection.next() {
                None => break,
                Some(tuple) => tuple.to_string(),
            }
        }
        println!("Scaned\n");
    }

    println!("projection");

    {
        let mut projection: ProjectionExec = ProjectionExec::new(&mut shohin_tb_scan, vec!["shohin_name", "price"]);
        loop {
            match projection.next() {
                None => break,
                Some(tuple) => tuple.to_string(),
            }
        }
        println!("Scaned\n");
    }

    println!("aggregation\n");

    {
        let mut aggregation = AggregationExec::new(&mut shohin_tb_scan, vec![AggrCount::new(), AggrSum::new("price"), AggrAvg::new("price")]);
        loop {
            match aggregation.next() {
                None => break,
                Some(tuple) => tuple.to_string(),
            }
        }
        println!("Scaned\n");
    }
}

