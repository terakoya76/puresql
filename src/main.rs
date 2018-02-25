//use std::collections::HashMap;

mod field;
mod column;
mod tuple;
mod item;
mod table;
//mod relation;
mod allocator;
mod executor;
pub use field::field::Field;
pub use column::column::Column;
pub use column::range::Range;
pub use tuple::tuple::Tuple;
pub use item::item::Item;
pub use table::table::Table;
//pub use relation::relation::Relation;
pub use allocator::allocator::Allocator;
pub use executor::table_scan::TableScanExec;
pub use executor::join::NestedLoopJoinExec;

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

    println!("select");
    let mut tb_scan: TableScanExec = TableScanExec::new(&shohin, &shohin.name, vec![Range::new(0, 10)]);
    loop {
        match tb_scan.next() {
            None => break,
            Some(tuple) => tuple.to_string(),
        }
    }
    println!("Scaned\n");

    println!("joined select");
    let mut shohin_tb_scan: TableScanExec = TableScanExec::new(&shohin, &shohin.name, vec![Range::new(0, 10)]);
    let mut kubun_tb_scan: TableScanExec = TableScanExec::new(&kubun, &kubun.name, vec![Range::new(0,10)]);
    let mut joined_exec: NestedLoopJoinExec = NestedLoopJoinExec::new(shohin_tb_scan, kubun_tb_scan);
    let joined_tps: Vec<Tuple> = joined_exec.join();
    for tp in joined_tps.iter() {
        tp.to_string();
    }
    println!("Scaned\n");

    /*
    println!("\nequal");
    shohin.from().equal("shohin_name", "orange").to_string();
    */
}

