//use std::collections::HashMap;

mod field;
mod column;
mod tuple;
mod table;
//mod relation;
mod allocator;
mod executor;
pub use field::field::Field;
pub use column::column::Column;
pub use column::column::Range;
pub use tuple::tuple::Tuple;
pub use table::table::Table;
//pub use relation::relation::Relation;
pub use allocator::allocator::Allocator;
pub use executor::executor::TableScanExec;

fn main() {
    println!("Whole Table\n");
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

    println!("select\n");

    let mut tb_scan: TableScanExec = TableScanExec::new(&shohin, &shohin.name, vec![Range::new(1, 10)]);
    loop {
        match tb_scan.next() {
            None => break,
            Some(tuple) => tuple.to_string(),
        }
    }
    println!("Scaned");

    /*
    println!("\nleft join");
    tables.insert(kubun.clone().name, kubun.clone());
    shohin.from().left_join(kubun.from(), "kubun_id").to_string();

    println!("\nequal");
    shohin.from().equal("shohin_name", "orange").to_string();
    */
}

