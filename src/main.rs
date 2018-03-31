extern crate bincode;
#[macro_use]
extern crate serde_derive;
extern crate serde;

use std::collections::HashMap;

mod client;
mod database;
mod context;
mod storage;
mod columns;
mod tables;
mod data_type;
mod meta;
mod allocators;
mod executors;
mod parser;

// trait
pub use executors::scan_exec::ScanExec;

// struct
pub use client::Client;
pub use database::Database;
pub use context::Context;
pub use storage::b_tree::BTree;
pub use columns::column::Column;
pub use columns::range::Range;
pub use tables::field::Field;
pub use tables::tuple::Tuple;
pub use tables::table::Table;
pub use tables::memory_table::MemoryTable;
pub use tables::index::Index;
pub use data_type::DataType;
pub use meta::table_info::TableInfo;
pub use meta::column_info::ColumnInfo;
pub use meta::index_info::IndexInfo;
pub use allocators::allocator::Allocator;
pub use executors::table_scan::TableScanExec;
pub use executors::memory_table_scan::MemoryTableScanExec;
pub use executors::join::NestedLoopJoinExec;
pub use executors::selection::SelectionExec;
pub use executors::selector::{equal, lt, le, gt, ge};
pub use executors::projection::ProjectionExec;
pub use executors::aggregation::AggregationExec;
pub use executors::aggregator::{Aggregator, Count, Sum, Average};
pub use parser::statement::*;
pub use parser::parser::Parser;

fn main() {
    let db: Database = Database {
        id: 1,
        name: "test".to_string(),
        tables: HashMap::new(),
    };

    let ctx: Context = Context {
        db: Some(db),
        table_id_alloc: Allocator::new(1),
    };

    let mut client: Client = Client::new(ctx);

    println!("Table on memory");
    let mut alloc: Box<Allocator> = Allocator::new(1);

    client.handle_query("create table shohin ( shohin_id int, shohin_name char(10), kubun_id int, price int )");

    client.handle_query("insert into shohin ( shohin_id, shohin_name, kubun_id, price ) values ( 1, 'apple', 1, 300 )");
    client.handle_query("insert into shohin ( shohin_id, shohin_name, kubun_id, price ) values ( 2, 'orange', 1, 130)");
    client.handle_query("insert into shohin ( shohin_id, shohin_name, kubun_id, price ) values ( 3, 'cabbage', 2, 200 )");
    client.handle_query("insert into shohin ( shohin_id, shohin_name, kubun_id, price ) values ( 4, 'sea weed', 5, 250)");
    client.handle_query("insert into shohin ( shohin_id, shohin_name, kubun_id, price ) values ( 5, 'mushroom', 3, 100 )");
    println!("");

    client.handle_query("create table kubun ( kubun_id int, kubun_name char(10) )");

    client.handle_query("insert into kubun ( kubun_id, kubun_name) values ( 1, 'fruit' )");
    client.handle_query("insert into kubun ( kubun_id, kubun_name) values ( 2, 'vegetable' )");
    println!("");

    /*
    println!("table scan");
    let mut m_shohin_tb_scan: MemoryTableScanExec = MemoryTableScanExec::new(&mut m_shohin, vec![Range::new(0, 10)]);

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
    let mut m_kubun_tb_scan: MemoryTableScanExec = MemoryTableScanExec::new(&mut m_kubun, vec![Range::new(0, 10)]);

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
        let mut aggregation = AggregationExec::new(&mut m_shohin_tb_scan, vec![], vec![Count::new(), Sum::new("price"), Average::new("price")]);
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
        let mut grouped = AggregationExec::new(&mut m_shohin_tb_scan, vec!["price"], vec![Count::new(), Sum::new("price"), Average::new("price")]);
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

    /*
    println!("Table with index");
    let shohin_prepare: Client = Client::new("create table shohin ( shohin_id int, shohin_name char(10), kubun_id int, price int )");
    let mut shohin_info: TableInfo = match shohin_prepare.exec() {
        Ok(tbl_info) => tbl_info,
        _ => return,
    };
    IndexInfo::new(&mut shohin_info, vec!["shohin_id"], true);

    let mut shohin: Table = Table::new(&mut shohin_info);
    shohin.insert(vec![Field::set_u64(1), Field::set_str("apple"), Field::set_u64(1), Field::set_u64(300)]);
    shohin.insert(vec![Field::set_u64(2), Field::set_str("orange"), Field::set_u64(1), Field::set_u64(130)]);
    shohin.insert(vec![Field::set_u64(3), Field::set_str("cabbage"), Field::set_u64(2), Field::set_u64(200)]);
    shohin.insert(vec![Field::set_u64(4), Field::set_str("sea weed"), Field::set_u64(5), Field::set_u64(250)]);
    shohin.insert(vec![Field::set_u64(5), Field::set_str("mushroom"), Field::set_u64(3), Field::set_u64(100)]);
    shohin.print();
    println!("");

    let kubun_prepare: Client = Client::new("create table kubun ( kubun_id int, kubun_name char(10) )");
    let mut kubun_info: TableInfo = match kubun_prepare.exec() {
        Ok(tbl_info) => tbl_info,
        _ => return,
    };
    IndexInfo::new(&mut kubun_info, vec!["kubun_id"], true);

    let mut kubun: Table = Table::new(&mut kubun_info);
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
        let mut aggregation = AggregationExec::new(&mut shohin_tb_scan, vec![], vec![Count::new(), Sum::new("price"), Average::new("price")]);
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
        let mut grouped = AggregationExec::new(&mut shohin_tb_scan, vec!["price"], vec![Count::new(), Sum::new("price"), Average::new("price")]);
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
    */
    */
}

