use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;

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
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl Equal {
    pub fn new(left_column: &str, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<Equal> {
        Box::new(Equal {
            left_column: left_column.to_string(),
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
                let right_side: Field = tuple.fields[offset].clone();
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] == right_side
                    }
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref field) => {
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] == field.clone()
                    }
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
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl NotEqual {
    pub fn new(left_column: &str, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<NotEqual> {
        Box::new(NotEqual {
            left_column: left_column.to_string(),
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
                let right_side: Field = tuple.fields[offset].clone();
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] != right_side
                    }
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref field) => {
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] != field.clone()
                    }
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
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl LT {
    pub fn new(left_column: &str, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<LT> {
        Box::new(LT {
            left_column: left_column.to_string(),
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
                let right_side: Field = tuple.fields[offset].clone();
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] < right_side
                    }
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref field) => {
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] < field.clone()
                    }
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
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl LE {
    pub fn new(left_column: &str, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<LE> {
        Box::new(LE {
            left_column: left_column.to_string(),
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
                let right_side: Field = tuple.fields[offset].clone();
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] <= right_side
                    }
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref field) => {
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] <= field.clone()
                    }
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
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl GT {
    pub fn new(left_column: &str, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<GT> {
        Box::new(GT {
            left_column: left_column.to_string(),
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
                let right_side: Field = tuple.fields[offset].clone();
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] > right_side
                    }
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref field) => {
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] > field.clone()
                    }
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
    pub left_column: String,
    pub right_column_offset: Option<usize>,
    pub scholar: Option<Field>,
}

impl GE {
    pub fn new(left_column: &str, right_column_offset: Option<usize>, scholar: Option<Field>) -> Box<GE> {
        Box::new(GE {
            left_column: left_column.to_string(),
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
                let right_side: Field = tuple.fields[offset].clone();
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] >= right_side
                    }
                }
            },
        }

        match self.scholar {
            None => {},
            Some(ref field) => {
                for column in columns {
                    if self.left_column == column.name {
                        return tuple.fields[column.offset] >= field.clone()
                    }
                }
            },
        }

        false
    }

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }
}

