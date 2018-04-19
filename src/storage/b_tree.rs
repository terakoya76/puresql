use bincode::{serialize, deserialize};

use std::io::prelude::*;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, SeekFrom, Error};
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct BTree {
    pub tree: BTreeMap<usize, RecordAddress>,
    pub file_path: String,
    pub datus: File,
}

impl BTree {
    pub fn new(file_path: &str) -> Result<BTree, Error> {
        let mut datus_path: String = file_path.to_string();
        datus_path.push_str(".datus");
        let datus: File = try!(OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(true)
                                .open(datus_path));

        let mut file_path: String = file_path.to_string();
        file_path.push_str(".btree");

        let mut buf: Vec<u8> = Vec::new();
        let mut btree_dump: File = try!(OpenOptions::new()
                                                .read(true)
                                                .write(true)
                                                .create(true)
                                                .open(&file_path));
        let _ = btree_dump.seek(SeekFrom::Start(0));
        let _ = btree_dump.read_to_end(&mut buf);
        let btree: BTreeMap<usize, RecordAddress> = match deserialize(&buf) {
            Ok(btree) => btree,
            Err(e) => {
                println!("{:?}", e);
                BTreeMap::new()
            },
        };

        Ok(BTree {
            tree: btree,
            file_path: file_path,
            datus: datus,
        })
    }

    pub fn insert(&mut self, record_id: usize, data: &[u8]) {
        if self.tree.contains_key(&record_id) {
            return ();
        }

        let offset = match self.datus.seek(SeekFrom::End(0)) {
            Ok(offset) => offset,
            Err(e) => {
                println!("{:?}", e);
                return ();
            },
        };

        match self.datus.write_all(data) {
            Ok(_void) => {
                let r_addr: RecordAddress = RecordAddress {
                    addr: offset,
                    size: data.len().clone() as u64,
                };
                &mut self.tree.insert(record_id, r_addr);
            },
            Err(e) => println!("{:?}", e),
        }
    }

    pub fn get_record(&mut self, record_id: usize) -> Vec<u8> {
        match self.tree.get(&record_id) {
            None => Vec::new(),
            Some(r_addr) => {
                let mut buf: Vec<u8> = vec![0; r_addr.size as usize];
                let _ = self.datus.seek(SeekFrom::Start(r_addr.addr));
                let _ = self.datus.read_exact(&mut buf);
                buf
            },
        }
    }
}

impl Drop for BTree {
    fn drop(&mut self) {
        let opt = OpenOptions::new()
                            .read(true)
                            .write(true)
                            .create(true)
                            .open(&self.file_path);

        let mut file: File = match opt {
            Ok(file) => file,
            Err(e) => {
                println!("{:?}", e);
                return ();
            },
        };

        let _ = file.seek(SeekFrom::Start(0));
        match serialize(&self.tree) {
            Ok(btree) => {
                let _ = file.write_all(&btree);
                ()
            },
            Err(e) => {
                println!("{:?}", e);
                ()
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordAddress {
    pub addr: u64,
    pub size: u64,
}
