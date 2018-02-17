use std::vec::Vec;
use std::collections::HashMap;
use std::borrow::ToOwned;

mod field;
mod column;
mod tuple;
mod table;
//mod relation;
mod allocator;
pub use field::field::Field;
pub use column::column::Column;
pub use tuple::tuple::Tuple;
pub use table::table::Table;
//pub use relation::relation::Relation;
pub use allocator::allocator::Allocator;

fn main() {
    println!("\nWhole Table");
    let alloc: Allocator = Allocator::new(1);
    let mut shohin: Table = Table::create(1, "shohin", vec!["shohin_id", "shohin_name", "kubun_id", "price"], alloc);
    let tuple: Tuple = Tuple::new(1, vec![Field::set_u64(1), Field::set_str("apple"), Field::set_u64(1), Field::set_u64(300)]);
    shohin.insert(tuple);
    //shohin.insert(vec!["1", "apple", "1", "300"]);
    //shohin.insert(vec!["2", "orange", "1", "130"]);
    //shohin.insert(vec!["3", "cabbage", "2", "200"]);
    //shohin.insert(vec!["4", "sea weed", "None", "250"]);
    //shohin.insert(vec!["5", "mushroom", "3", "100"]);
    shohin.to_string();

    /*
    let mut kubun: Table = Table::create("kubun", vec!["kubun_id", "kubun_name"]);
    kubun.insert(vec!["1", "fruit"]);
    kubun.insert(vec!["2", "vegetable"]);

    println!("\nselect");
    let mut tables = HashMap::new();
    tables.insert(shohin.clone().name, shohin.clone());
    shohin.from().select(vec!["shohin_id", "shohin_name"]).to_string();

    println!("\nleft join");
    tables.insert(kubun.clone().name, kubun.clone());
    shohin.from().left_join(kubun.from(), "kubun_id").to_string();

    println!("\nequal");
    shohin.from().equal("shohin_name", "orange").to_string();
    */
}

