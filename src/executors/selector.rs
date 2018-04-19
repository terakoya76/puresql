use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;
use parser::statement::*;

pub trait Selector {
    fn evaluate(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError>;
    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool;
    fn is_false(&self, tuple: &Tuple, columns: &[Column]) -> bool;
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
    pub right_hand: Option<Target>,
    pub scholar: Option<Field>,
}

impl Equal {
    pub fn new(left_hand: Target, right_hand: Option<Target>, scholar: Option<Field>) -> Box<Equal> {
        Box::new(Equal {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_hand: right_hand,
            scholar: scholar,
        })
    }
}

impl Selector for Equal {
    fn evaluate(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {},
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(tuple, columns, right_hand.table_name.clone(), right_hand.name.clone()));
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                return Ok(left_side == right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                Ok(left_side == right_side)
            },
        }
    }

    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => false
        }
    }

    fn is_false(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => true
        }
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct NotEqual {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_hand: Option<Target>,
    pub scholar: Option<Field>,
}

impl NotEqual {
    pub fn new(left_hand: Target, right_hand: Option<Target>, scholar: Option<Field>) -> Box<NotEqual> {
        Box::new(NotEqual {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_hand: right_hand,
            scholar: scholar,
        })
    }
}

impl Selector for NotEqual {
    fn evaluate(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {},
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(tuple, columns, right_hand.table_name.clone(), right_hand.name.clone()));
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                return Ok(left_side != right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                Ok(left_side != right_side)
            },
        }
    }

    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => false
        }
    }

    fn is_false(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => true
        }
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct LT {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_hand: Option<Target>,
    pub scholar: Option<Field>,
}

impl LT {
    pub fn new(left_hand: Target, right_hand: Option<Target>, scholar: Option<Field>) -> Box<LT> {
        Box::new(LT {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_hand: right_hand,
            scholar: scholar,
        })
    }
}

impl Selector for LT {
    fn evaluate(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {},
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(tuple, columns, right_hand.table_name.clone(), right_hand.name.clone()));
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                return Ok(left_side < right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                Ok(left_side < right_side)
            },
        }
    }

    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => false
        }
    }

    fn is_false(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => true
        }
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct LE {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_hand: Option<Target>,
    pub scholar: Option<Field>,
}

impl LE {
    pub fn new(left_hand: Target, right_hand: Option<Target>, scholar: Option<Field>) -> Box<LE> {
        Box::new(LE {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_hand: right_hand,
            scholar: scholar,
        })
    }
}

impl Selector for LE {
    fn evaluate(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {},
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(tuple, columns, right_hand.table_name.clone(), right_hand.name.clone()));
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                return Ok(left_side <= right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                Ok(left_side <= right_side)
            },
        }
    }

    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => false
        }
    }

    fn is_false(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => true
        }
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct GT {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_hand: Option<Target>,
    pub scholar: Option<Field>,
}

impl GT {
    pub fn new(left_hand: Target, right_hand: Option<Target>, scholar: Option<Field>) -> Box<GT> {
        Box::new(GT {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_hand: right_hand,
            scholar: scholar,
        })
    }
}

impl Selector for GT {
    fn evaluate(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {},
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(tuple, columns, right_hand.table_name.clone(), right_hand.name.clone()));
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                return Ok(left_side > right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                Ok(left_side > right_side)
            },
        }
    }

    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => false
        }
    }

    fn is_false(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => true
        }
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct GE {
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_hand: Option<Target>,
    pub scholar: Option<Field>,
}

impl GE {
    pub fn new(left_hand: Target, right_hand: Option<Target>, scholar: Option<Field>) -> Box<GE> {
        Box::new(GE {
            left_table: left_hand.table_name,
            left_column: left_hand.name,
            right_hand: right_hand,
            scholar: scholar,
        })
    }
}

impl Selector for GE {
    fn evaluate(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {},
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(tuple, columns, right_hand.table_name.clone(), right_hand.name.clone()));
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                return Ok(left_side >= right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table, self.left_column));
                Ok(left_side >= right_side)
            },
        }
    }

    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => false
        }
    }

    fn is_false(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.evaluate(tuple, columns) {
            Ok(b) => b,
            _ => true
        }
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

fn find_field(tuple: &Tuple, columns: &[Column], table_name: Option<String>, column_name: String) -> Result<Field, SelectorError> {
    for column in columns {
        if column_name != column.name {
            continue;
        }

        match table_name {
            None => return Ok(tuple.fields[column.offset]),
            Some(ref tbl_name) => {
                if tbl_name == &column.table_name {
                    return Ok(tuple.fields[column.offset]);
                }
            },
        }
    }

    Err(SelectorError::ColumnNotFoundError)
}

#[derive(Debug, PartialEq)]
pub enum SelectorError {
    ColumnNotFoundError,
    UnexpectedRightHandError,
}
