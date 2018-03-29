use bincode::serialize;

use std::io::prelude::*;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, SeekFrom, Error};
use std::collections::BTreeMap;

use tables::tuple::Tuple;

#[derive(Debug)]
pub struct BTree {
    pub tree: BTreeMap<usize, RecordAddress>,
    pub file: File,
    pub cursor: u64,
}

impl BTree {
    pub fn new(file_path: &str) -> Result<BTree, Error> {
        let file: File = try!(OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(true)
                                .open(file_path));

        Ok(BTree {
            tree: BTreeMap::new(),
            file: file,
            cursor: 0,
        })
    }

    pub fn insert(&mut self, record_id: usize, data: &[u8]) {
        if self.tree.contains_key(&record_id) {
            return ();
        }

        match self.file.write(data) {
            Ok(size) => {
                let r_addr: RecordAddress = RecordAddress {
                    addr: self.cursor,
                    size: size as u64,
                };
                &mut self.tree.insert(record_id, r_addr);
                self.cursor += size as u64;
            },
            _ => {},
        }
    }

    pub fn get_record(&mut self, record_id: usize) -> Vec<u8> {
        match self.tree.get(&record_id) {
            None => {
                println!("not found");
                Vec::new()
            },
            Some(r_addr) => {
                let mut buf: Vec<u8> = vec![0; r_addr.size as usize];
                self.file.seek(SeekFrom::Start(r_addr.addr));
                self.file.read_exact(&mut buf);
                buf
            },
        }
    }
}

pub fn gen_blank_tuple() -> Tuple {
    Tuple::new(vec![])
}

#[derive(Debug, Clone)]
pub struct RecordAddress {
    pub addr: u64,
    pub size: u64,
}

