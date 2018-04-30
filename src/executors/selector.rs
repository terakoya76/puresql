use std::fmt;
use std::any::Any;

use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;
use parser::statement::*;

pub trait Selector {
    fn evaluate(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError>;
    fn is_true(&self, tuple: &Tuple, columns: &[Column]) -> bool;
    fn box_clone(&self) -> Box<Selector>;
    fn as_any(&self) -> &Any;
    fn compare(&self, &Selector) -> bool;
}

impl fmt::Debug for Box<Selector> {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}

impl Clone for Box<Selector> {
    fn clone(&self) -> Box<Selector> {
        self.box_clone()
    }
}

impl PartialEq for Box<Selector> {
    fn eq(&self, other: &Box<Selector>) -> bool {
        let tmp = other.clone();
        self.compare(&*tmp)
    }
}

#[derive(Debug, Clone, PartialEq)]
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
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
                return Ok(left_side == right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
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

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &Any {
        self
    }

    fn compare(&self, other: &Selector) -> bool {
        other.as_any()
             .downcast_ref::<Equal>()
             .map_or(false, |a| self == a)
    }
}

#[derive(Debug, Clone, PartialEq)]
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
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
                return Ok(left_side != right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
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

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &Any {
        self
    }

    fn compare(&self, other: &Selector) -> bool {
        other.as_any()
             .downcast_ref::<NotEqual>()
             .map_or(false, |a| self == a)
    }
}

#[derive(Debug, Clone, PartialEq)]
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
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
                return Ok(left_side < right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
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

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &Any {
        self
    }

    fn compare(&self, other: &Selector) -> bool {
        other.as_any()
             .downcast_ref::<LT>()
             .map_or(false, |a| self == a)
    }
}

#[derive(Debug, Clone, PartialEq)]
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
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
                return Ok(left_side <= right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
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

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &Any {
        self
    }

    fn compare(&self, other: &Selector) -> bool {
        other.as_any()
             .downcast_ref::<LE>()
             .map_or(false, |a| self == a)
    }
}

#[derive(Debug, Clone, PartialEq)]
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
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
                return Ok(left_side > right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
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

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &Any {
        self
    }

    fn compare(&self, other: &Selector) -> bool {
        other.as_any()
             .downcast_ref::<GT>()
             .map_or(false, |a| self == a)
    }
}

#[derive(Debug, Clone, PartialEq)]
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
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
                return Ok(left_side >= right_side);
            },
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(tuple, columns, self.left_table.clone(), self.left_column.clone()));
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

    fn box_clone(&self) -> Box<Selector> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &Any {
        self
    }

    fn compare(&self, other: &Selector) -> bool {
        other.as_any()
             .downcast_ref::<GE>()
             .map_or(false, |a| self == a)
    }
}

fn find_field(tuple: &Tuple, columns: &[Column], table_name: Option<String>, column_name: String) -> Result<Field, SelectorError> {
    for column in columns {
        if column_name != column.name {
            continue;
        }

        match table_name {
            None => return Ok(tuple.fields[column.offset].clone()),
            Some(ref tbl_name) => {
                if tbl_name == &column.table_name {
                    return Ok(tuple.fields[column.offset].clone());
                }
            },
        }
    }
    Err(SelectorError::ColumnNotFoundError)
}

pub fn build_selectors(condition: Conditions, is_or: bool) -> Result<Vec<Box<Selector>>, SelectorError> {
    match condition {
        Conditions::And(c1, c2) => {
            let mut selectors1: Vec<Box<Selector>> = try!(build_selectors(*c1, false));
            let mut selectors2: Vec<Box<Selector>> = try!(build_selectors(*c2, false));
            selectors1.append(&mut selectors2);
            Ok(selectors1)
        },

        Conditions::Or(c1, c2) => {
            let mut selectors1: Vec<Box<Selector>> = try!(build_selectors(*c1, true));
            let mut selectors2: Vec<Box<Selector>> = try!(build_selectors(*c2, true));
            selectors1.append(&mut selectors2);
            Ok(selectors1)
        },

        Conditions::Leaf(condition) => {
            match condition.op {
                Operator::Equ => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![NotEqual::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![NotEqual::new(condition.left, Some(t), None)]),
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![Equal::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![Equal::new(condition.left, Some(t), None)]),
                        }
                    }
                },

                Operator::NEqu => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![Equal::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![Equal::new(condition.left, Some(t), None)]),
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![NotEqual::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![NotEqual::new(condition.left, Some(t), None)]),
                        }
                    }
                },

                Operator::GT => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![LE::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![LE::new(condition.left, Some(t), None)]),
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![GT::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![GT::new(condition.left, Some(t), None)]),
                        }
                    }
                },

                Operator::LT => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![GE::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![GE::new(condition.left, Some(t), None)]),
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![LT::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![LT::new(condition.left, Some(t), None)]),
                        }
                    }
                },

                Operator::GE => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![LT::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![LT::new(condition.left, Some(t), None)]),
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![GE::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![GE::new(condition.left, Some(t), None)]),
                        }
                    }
                },

                Operator::LE => {
                    if is_or {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![GT::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![GT::new(condition.left, Some(t), None)]),
                        }
                    } else {
                        match condition.right {
                            Comparable::Lit(l) => Ok(vec![LE::new(condition.left, None, Some(l.into()))]),
                            Comparable::Target(t) => Ok(vec![LE::new(condition.left, Some(t), None)]),
                        }
                    }
                },
            }
        },
    }
}

#[derive(Debug, PartialEq)]
pub enum SelectorError {
    ColumnNotFoundError,
    UnexpectedRightHandError,
}

#[cfg(test)]
mod tests {
    use super::*;
    use parser::token::*;
    use data_type::DataType;

    fn gen_columns() -> Vec<Column> {
        let column_defs = &[
            ("shohin_id".to_owned(), DataType::Int),
            ("shohin_name".to_owned(), DataType::Char(10)),
            ("kubun_id".to_owned(), DataType::Int),
            ("price".to_owned(), DataType::Int),
        ];
        
        column_defs.into_iter().enumerate().map(|(i, col)| Column {
            table_name: "shohin".to_owned(),
            name: col.clone().0,
            dtype: col.clone().1,
            offset: i,
        }).collect()
    }

    fn gen_tuple() -> Tuple {
        let fields: Vec<Field> = vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(1),
            Field::set_i64(300)
        ];

        Tuple::new(fields)
    }

    #[test]
    fn test_find_field() {
        let field = find_field(&gen_tuple(), &gen_columns(), Some("shohin".to_owned()), "price".to_owned());
        assert_eq!(field, Ok(Field::set_i64(300)));

        let field = find_field(&gen_tuple(), &gen_columns(), None, "price".to_owned());
        assert_eq!(field, Ok(Field::set_i64(300)));

        let field = find_field(&gen_tuple(), &gen_columns(), None, "fictional_column".to_owned());
        assert!(field.is_err());
    }

    fn gen_left_hand4str() -> Target {
        Target {
            table_name: Some("shohin".to_owned()),
            name: "shohin_name".to_owned(),
        }
    }

    fn gen_left_hand4int() -> Target {
        Target {
            table_name: Some("shohin".to_owned()),
            name: "price".to_owned(),
        }
    }

    fn gen_right_column4str() -> Target {
        Target {
            table_name: Some("shohin".to_owned()),
            name: "shohin_name".to_owned(),
        }
    }

    fn gen_right_column4int() -> Target {
        Target {
            table_name: Some("shohin".to_owned()),
            name: "price".to_owned(),
        }
    }

    #[test]
    fn build_equal_leaf() {
        let left_hand: Target = gen_left_hand4str();
        let right_column: Target = gen_right_column4str();
        let right_literal: Literal = Literal::String("apple".to_owned());

        // build w/ column right hand
        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::Equ,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition, false).unwrap();
        assert_eq!(selectors, vec![Equal::new(left_hand.clone(), Some(right_column), None) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), true);
        
        // build w/ scholar right hand
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::Equ,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition, false).unwrap();
        assert_eq!(selectors, vec![Equal::new(left_hand.clone(), None, Some(Field::set_str("apple"))) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), true);
    }

    #[test]
    fn build_not_equal_leaf() {
        let left_hand: Target = gen_left_hand4str();
        let right_column: Target = Target {
            table_name: Some("shohin".to_owned()),
            name: "shohin_id".to_owned(),
        };
        let right_literal: Literal = Literal::String("grape".to_owned());

        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::NEqu,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition, false).unwrap();
        assert_eq!(selectors, vec![NotEqual::new(left_hand.clone(), Some(right_column), None) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), true);
        
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::NEqu,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition, false).unwrap();
        assert_eq!(selectors, vec![NotEqual::new(left_hand.clone(), None, Some(Field::set_str("grape"))) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), true);
    }

    #[test]
    fn build_gt_leaf() {
        let left_hand: Target = gen_left_hand4int();
        let right_column: Target = gen_right_column4int();
        let right_literal: Literal = Literal::Int(200);

        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GT,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition, false).unwrap();
        assert_eq!(selectors, vec![GT::new(left_hand.clone(), Some(right_column), None) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), false);
        
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GT,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition, false).unwrap();
        assert_eq!(selectors, vec![GT::new(left_hand.clone(), None, Some(Field::set_i64(200))) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), true);
    }

    #[test]
    fn build_lt_leaf() {
        let left_hand: Target = gen_left_hand4int();
        let right_column: Target = gen_right_column4int();
        let right_literal: Literal = Literal::Int(400);

        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LT,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition, false).unwrap();
        assert_eq!(selectors, vec![LT::new(left_hand.clone(), Some(right_column), None) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), false);
        
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LT,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition, false).unwrap();
        assert_eq!(selectors, vec![LT::new(left_hand.clone(), None, Some(Field::set_i64(400))) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), true);
    }

    #[test]
    fn build_ge_leaf() {
        let left_hand: Target = gen_left_hand4int();
        let right_column: Target = gen_right_column4int();
        let right_literal: Literal = Literal::Int(200);

        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GE,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition, false).unwrap();
        assert_eq!(selectors, vec![GE::new(left_hand.clone(), Some(right_column), None) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), true);
        
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GE,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition, false).unwrap();
        assert_eq!(selectors, vec![GE::new(left_hand.clone(), None, Some(Field::set_i64(200))) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), true);
    }

    #[test]
    fn build_le_leaf() {
        let left_hand: Target = gen_left_hand4int();
        let right_column: Target = gen_right_column4int();
        let right_literal: Literal = Literal::Int(400);

        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LE,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition, false).unwrap();
        assert_eq!(selectors, vec![LE::new(left_hand.clone(), Some(right_column), None) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert_eq!(selector.is_true(&gen_tuple(), &gen_columns()), true);
        
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LE,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition, false).unwrap();
        assert_eq!(selectors, vec![LE::new(left_hand.clone(), None, Some(Field::set_i64(400))) as Box<Selector>]);

        let selector = &selectors.clone()[0];
        assert!(selector.is_true(&gen_tuple(), &gen_columns()), true);
    }
}
