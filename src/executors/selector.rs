use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;
use parser::statement::*;

pub trait Selector {
    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool;
    fn box_clone(&self) -> Box<Selector>;
}

impl Clone for Box<Selector> {
    fn clone(&self) -> Box<Selector> {
        self.box_clone()
    }
}

#[derive(Debug, Clone)]
pub struct Equal {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl Equal {
    pub fn new(left_hand: Target, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<Equal> {
        Box::new(Equal {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_column_offset: right_column_offset,
            scholar: scholar,
        })
    }
}

impl Selector for Equal {
    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.right_column_offset {
            None => {},
            Some(offset) => {
                let ref right_side: Field = tuple.fields[offset];
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side == right_side,
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref right_side) => {
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side == right_side,
                }
            },
        }

        false
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct NotEqual {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl NotEqual {
    pub fn new(left_hand: Target, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<NotEqual> {
        Box::new(NotEqual {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_column_offset: right_column_offset,
            scholar: scholar,
        })
    }
}

impl Selector for NotEqual {
    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.right_column_offset {
            None => {},
            Some(offset) => {
                let ref right_side: Field = tuple.fields[offset];
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side != right_side,
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref right_side) => {
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side != right_side,
                }
            },
        }

        false
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct LT {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl LT {
    pub fn new(left_hand: Target, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<LT> {
        Box::new(LT {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_column_offset: right_column_offset,
            scholar: scholar,
        })
    }
}

impl Selector for LT {
    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.right_column_offset {
            None => {},
            Some(offset) => {
                let ref right_side: Field = tuple.fields[offset];
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side < right_side,
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref right_side) => {
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side < right_side,
                }
            },
        }

        false
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct LE {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl LE {
    pub fn new(left_hand: Target, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<LE> {
        Box::new(LE {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_column_offset: right_column_offset,
            scholar: scholar,
        })
    }
}

impl Selector for LE {
    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.right_column_offset {
            None => {},
            Some(offset) => {
                let ref right_side: Field = tuple.fields[offset];
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side <= right_side,
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref right_side) => {
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side <= right_side,
                }
            },
        }

        false
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct GT {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl GT {
    pub fn new(left_hand: Target, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<GT> {
        Box::new(GT {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_column_offset: right_column_offset,
            scholar: scholar,
        })
    }
}

impl Selector for GT {
    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.right_column_offset {
            None => {},
            Some(offset) => {
                let ref right_side: Field = tuple.fields[offset];
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side > right_side,
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref right_side) => {
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side > right_side,
                }
            },
        }

        false
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct GE {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl GE {
    pub fn new(left_hand: Target, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<GE> {
        Box::new(GE {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_column_offset: right_column_offset,
            scholar: scholar,
        })
    }
}

impl Selector for GE {
    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.right_column_offset {
            None => {},
            Some(offset) => {
                let ref right_side: Field = tuple.fields[offset];
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side >= right_side,
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref right_side) => {
                let left_side_opt: Option<Field> = find_field(tuple, columns, self.left_table, self.left_column);
                match left_side_opt {
                    None => return false,
                    Some(ref left_side) => return left_side >= right_side,
                }
            },
        }

        false
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

fn find_field(tuple: &Tuple, columns: &[Column], left_table: Option<String>, left_column: String) -> Option<Field> {
    for column in columns {
        if left_column != column.name {
            continue;
        }

        match left_table {
            None => return Some(tuple.fields[column.offset]),
            Some(ref table_name) => {
                if table_name == &column.table_name {
                    return Some(tuple.fields[column.offset]);
                }
            },
        }
    }

    None
}

