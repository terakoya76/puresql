use columns::column::Column;
use tables::tuple::Tuple;
use tables::field::Field;
use parser::statement::*;

#[derive(Debug, Clone, PartialEq)]
pub enum Selectors {
    Leaf(Selector),
    And(Box<Selectors>, Box<Selectors>),
    Or(Box<Selectors>, Box<Selectors>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Selector {
    pub kind: Operator,
    pub left_table: Option<String>,
    pub left_column: String,
    pub right_hand: Option<Target>,
    pub scholar: Option<Field>,
}

impl Selector {
    pub fn eval(&self, tuple: &Tuple, columns: &[Column]) -> bool {
        match self.kind {
            Operator::Equ => self.eval_equ(tuple, columns).unwrap_or(false),
            Operator::NEqu => self.eval_nequ(tuple, columns).unwrap_or(false),
            Operator::GE => self.eval_ge(tuple, columns).unwrap_or(false),
            Operator::LE => self.eval_le(tuple, columns).unwrap_or(false),
            Operator::GT => self.eval_gt(tuple, columns).unwrap_or(false),
            Operator::LT => self.eval_lt(tuple, columns).unwrap_or(false),
        }
    }

    fn eval_equ(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {}
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(
                    tuple,
                    columns,
                    right_hand.table_name.clone(),
                    right_hand.name.clone()
                ));
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                return Ok(left_side == right_side);
            }
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                Ok(left_side == right_side)
            }
        }
    }

    fn eval_nequ(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {}
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(
                    tuple,
                    columns,
                    right_hand.table_name.clone(),
                    right_hand.name.clone()
                ));
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                return Ok(left_side != right_side);
            }
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                Ok(left_side != right_side)
            }
        }
    }

    fn eval_ge(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {}
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(
                    tuple,
                    columns,
                    right_hand.table_name.clone(),
                    right_hand.name.clone()
                ));
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                return Ok(left_side >= right_side);
            }
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                Ok(left_side >= right_side)
            }
        }
    }

    fn eval_le(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {}
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(
                    tuple,
                    columns,
                    right_hand.table_name.clone(),
                    right_hand.name.clone()
                ));
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                return Ok(left_side <= right_side);
            }
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                Ok(left_side <= right_side)
            }
        }
    }

    fn eval_gt(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {}
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(
                    tuple,
                    columns,
                    right_hand.table_name.clone(),
                    right_hand.name.clone()
                ));
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                return Ok(left_side > right_side);
            }
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                Ok(left_side > right_side)
            }
        }
    }

    fn eval_lt(&self, tuple: &Tuple, columns: &[Column]) -> Result<bool, SelectorError> {
        match self.right_hand {
            None => {}
            Some(ref right_hand) => {
                let ref right_side: Field = try!(find_field(
                    tuple,
                    columns,
                    right_hand.table_name.clone(),
                    right_hand.name.clone()
                ));
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                return Ok(left_side < right_side);
            }
        }

        match self.scholar {
            None => Err(SelectorError::UnexpectedRightHandError),
            Some(ref right_side) => {
                let ref left_side: Field = try!(find_field(
                    tuple,
                    columns,
                    self.left_table.clone(),
                    self.left_column.clone()
                ));
                Ok(left_side < right_side)
            }
        }
    }
}

fn find_field(
    tuple: &Tuple,
    columns: &[Column],
    table_name: Option<String>,
    column_name: String,
) -> Result<Field, SelectorError> {
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
            }
        }
    }
    Err(SelectorError::ColumnNotFoundError)
}

pub fn build_selectors(condition: Conditions) -> Result<Selectors, SelectorError> {
    match condition {
        Conditions::And(c1, c2) => {
            let selectors1: Selectors = try!(build_selectors(*c1));
            let selectors2: Selectors = try!(build_selectors(*c2));
            Ok(Selectors::And(Box::new(selectors1), Box::new(selectors2)))
        }

        Conditions::Or(c1, c2) => {
            let selectors1: Selectors = try!(build_selectors(*c1));
            let selectors2: Selectors = try!(build_selectors(*c2));
            Ok(Selectors::Or(Box::new(selectors1), Box::new(selectors2)))
        }

        Conditions::Leaf(condition) => {
            let kind: Operator = condition.op;
            match condition.right {
                Comparable::Lit(l) => Ok(Selectors::Leaf(Selector {
                    kind: kind,
                    left_table: condition.left.table_name,
                    left_column: condition.left.name,
                    right_hand: None,
                    scholar: Some(l.into()),
                })),
                Comparable::Target(t) => Ok(Selectors::Leaf(Selector {
                    kind: kind,
                    left_table: condition.left.table_name,
                    left_column: condition.left.name,
                    right_hand: Some(t),
                    scholar: None,
                })),
            }
        }
    }
}

pub fn eval_selectors(selectors: Selectors, tuple: &Tuple, columns: &[Column]) -> bool {
    match selectors {
        Selectors::Leaf(c) => c.eval(tuple, columns),
        Selectors::And(c1, c2) => {
            eval_selectors(*c1, tuple, columns) && eval_selectors(*c2, tuple, columns)
        }
        Selectors::Or(c1, c2) => {
            eval_selectors(*c1, tuple, columns) || eval_selectors(*c2, tuple, columns)
        }
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
            ("price".to_owned(), DataType::Int),
            ("prev_price".to_owned(), DataType::Int),
        ];

        column_defs
            .into_iter()
            .enumerate()
            .map(|(i, col)| Column {
                table_name: "shohin".to_owned(),
                name: col.clone().0,
                dtype: col.clone().1,
                offset: i,
            })
            .collect()
    }

    fn gen_left_column() -> Target {
        Target {
            table_name: Some("shohin".to_owned()),
            name: "price".to_owned(),
        }
    }

    fn gen_right_column() -> Target {
        Target {
            table_name: Some("shohin".to_owned()),
            name: "prev_price".to_owned(),
        }
    }

    #[test]
    fn test_find_field() {
        let tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(100),
        ]);

        let field = find_field(
            &tuple.clone(),
            &gen_columns(),
            Some("shohin".to_owned()),
            "price".to_owned(),
        );
        assert_eq!(field, Ok(Field::set_i64(300)));

        let field = find_field(&tuple.clone(), &gen_columns(), None, "price".to_owned());
        assert_eq!(field, Ok(Field::set_i64(300)));

        let field = find_field(
            &tuple.clone(),
            &gen_columns(),
            None,
            "fictional_column".to_owned(),
        );
        assert!(field.is_err());
    }

    #[test]
    fn test_build_column_equal_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::Equ,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(condition).unwrap();

        let equ = Selectors::Leaf(Selector {
            kind: Operator::Equ,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        assert_eq!(selectors, equ);
    }

    #[test]
    fn test_build_lit_equal_leaf() {
        let left_hand: Target = gen_left_column();
        let right_literal: Literal = Literal::Int(300);

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::Equ,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(condition).unwrap();

        let equ = Selectors::Leaf(Selector {
            kind: Operator::Equ,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        assert_eq!(selectors, equ);
    }

    #[test]
    fn test_eval_column_equal_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();
        let equ = Selectors::Leaf(Selector {
            kind: Operator::Equ,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(equ.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(100),
        ]);
        assert_eq!(
            eval_selectors(equ.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_eval_lit_equal_leaf() {
        let left_hand: Target = gen_left_column();
        let equ = Selectors::Leaf(Selector {
            kind: Operator::Equ,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(equ.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(200),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(equ.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_build_column_not_equal_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::NEqu,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(condition).unwrap();

        let nequ = Selectors::Leaf(Selector {
            kind: Operator::NEqu,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        assert_eq!(selectors, nequ);
    }

    #[test]
    fn test_build_lit_not_equal_leaf() {
        let left_hand: Target = gen_left_column();
        let right_literal: Literal = Literal::Int(300);

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::NEqu,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(condition).unwrap();

        let nequ = Selectors::Leaf(Selector {
            kind: Operator::NEqu,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        assert_eq!(selectors, nequ);
    }

    #[test]
    fn test_eval_column_not_equal_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();
        let nequ = Selectors::Leaf(Selector {
            kind: Operator::NEqu,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(200),
        ]);
        assert_eq!(
            eval_selectors(nequ.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(nequ.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_eval_lit_not_equal_leaf() {
        let left_hand: Target = gen_left_column();
        let nequ = Selectors::Leaf(Selector {
            kind: Operator::NEqu,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(200),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(nequ.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(nequ.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_build_column_gt_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GT,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(condition).unwrap();

        let gt = Selectors::Leaf(Selector {
            kind: Operator::GT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        assert_eq!(selectors, gt);
    }

    #[test]
    fn test_build_lit_gt_leaf() {
        let left_hand: Target = gen_left_column();
        let right_literal: Literal = Literal::Int(300);

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GT,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(condition).unwrap();

        let gt = Selectors::Leaf(Selector {
            kind: Operator::GT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        assert_eq!(selectors, gt);
    }

    #[test]
    fn test_eval_column_gt_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();
        let gt = Selectors::Leaf(Selector {
            kind: Operator::GT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(200),
        ]);
        assert_eq!(
            eval_selectors(gt.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(gt.clone(), &falsey_tuple, &gen_columns()),
            false
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(400),
        ]);
        assert_eq!(
            eval_selectors(gt.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_eval_lit_gt_leaf() {
        let left_hand: Target = gen_left_column();
        let gt = Selectors::Leaf(Selector {
            kind: Operator::GT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(400),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(gt.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(gt.clone(), &falsey_tuple, &gen_columns()),
            false
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(400),
        ]);
        assert_eq!(
            eval_selectors(gt.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_build_column_lt_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LT,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(condition).unwrap();

        let lt = Selectors::Leaf(Selector {
            kind: Operator::LT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        assert_eq!(selectors, lt);
    }

    #[test]
    fn test_build_lit_lt_leaf() {
        let left_hand: Target = gen_left_column();
        let right_literal: Literal = Literal::Int(300);

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LT,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(condition).unwrap();

        let lt = Selectors::Leaf(Selector {
            kind: Operator::LT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        assert_eq!(selectors, lt);
    }

    #[test]
    fn test_eval_column_lt_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();
        let lt = Selectors::Leaf(Selector {
            kind: Operator::LT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(200),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(lt.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(lt.clone(), &falsey_tuple, &gen_columns()),
            false
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(400),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(lt.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_eval_lit_lt_leaf() {
        let left_hand: Target = gen_left_column();
        let lt = Selectors::Leaf(Selector {
            kind: Operator::LT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(200),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(lt.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(lt.clone(), &falsey_tuple, &gen_columns()),
            false
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(400),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(lt.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_build_column_ge_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GE,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(condition).unwrap();

        let ge = Selectors::Leaf(Selector {
            kind: Operator::GE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        assert_eq!(selectors, ge);
    }

    #[test]
    fn test_build_lit_ge_leaf() {
        let left_hand: Target = gen_left_column();
        let right_literal: Literal = Literal::Int(300);

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GE,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(condition).unwrap();

        let ge = Selectors::Leaf(Selector {
            kind: Operator::GE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        assert_eq!(selectors, ge);
    }

    #[test]
    fn test_eval_column_ge_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();
        let ge = Selectors::Leaf(Selector {
            kind: Operator::GE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(400),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(ge.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(ge.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(200),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(ge.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_eval_lit_ge_leaf() {
        let left_hand: Target = gen_left_column();
        let ge = Selectors::Leaf(Selector {
            kind: Operator::GE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(400),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(ge.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(ge.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(200),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(ge.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_build_column_le_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LE,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(condition).unwrap();

        let le = Selectors::Leaf(Selector {
            kind: Operator::LE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        assert_eq!(selectors, le);
    }

    #[test]
    fn test_build_lit_le_leaf() {
        let left_hand: Target = gen_left_column();
        let right_literal: Literal = Literal::Int(300);

        let condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LE,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(condition).unwrap();

        let le = Selectors::Leaf(Selector {
            kind: Operator::LE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        assert_eq!(selectors, le);
    }

    #[test]
    fn test_eval_column_le_leaf() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();
        let le = Selectors::Leaf(Selector {
            kind: Operator::LE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(200),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(le.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(le.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(400),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(le.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_eval_lit_le_leaf() {
        let left_hand: Target = gen_left_column();
        let le = Selectors::Leaf(Selector {
            kind: Operator::LE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(300)),
        });

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(200),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(le.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(le.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(400),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(le.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_build_and_node() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();
        let right_literal: Literal = Literal::Int(200);

        let nequ: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::NEqu,
            right: Comparable::Target(right_column.clone()),
        });
        let gt: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GT,
            right: Comparable::Lit(right_literal),
        });
        let condition: Conditions = Conditions::And(Box::new(nequ), Box::new(gt));
        let built_selectors = build_selectors(condition).unwrap();

        let and = Selectors::And(
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::NEqu,
                left_table: left_hand.clone().table_name,
                left_column: left_hand.clone().name,
                right_hand: Some(right_column),
                scholar: None,
            })),
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::GT,
                left_table: left_hand.clone().table_name,
                left_column: left_hand.clone().name,
                right_hand: None,
                scholar: Some(Field::set_i64(200)),
            })),
        );
        assert_eq!(built_selectors, and);
    }

    #[test]
    fn test_eval_and_node() {
        let left_hand: Target = gen_left_column();
        let and = Selectors::And(
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::NEqu,
                left_table: left_hand.clone().table_name,
                left_column: left_hand.clone().name,
                right_hand: None,
                scholar: Some(Field::set_i64(300)),
            })),
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::GT,
                left_table: left_hand.clone().table_name,
                left_column: left_hand.clone().name,
                right_hand: None,
                scholar: Some(Field::set_i64(200)),
            })),
        );

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(250),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(and.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(100),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(and.clone(), &falsey_tuple, &gen_columns()),
            false
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(and.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }

    #[test]
    fn test_build_or_node() {
        let left_hand: Target = gen_left_column();
        let right_column: Target = gen_right_column();
        let right_literal: Literal = Literal::Int(200);

        let equ: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::Equ,
            right: Comparable::Target(right_column.clone()),
        });
        let gt: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GT,
            right: Comparable::Lit(right_literal),
        });
        let condition: Conditions = Conditions::Or(Box::new(equ), Box::new(gt));
        let built_selectors = build_selectors(condition).unwrap();

        let or = Selectors::Or(
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::Equ,
                left_table: left_hand.clone().table_name,
                left_column: left_hand.clone().name,
                right_hand: Some(right_column),
                scholar: None,
            })),
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::GT,
                left_table: left_hand.clone().table_name,
                left_column: left_hand.clone().name,
                right_hand: None,
                scholar: Some(Field::set_i64(200)),
            })),
        );
        assert_eq!(built_selectors, or);
    }

    #[test]
    fn test_eval_or_node() {
        let left_hand: Target = gen_left_column();
        let or = Selectors::Or(
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::Equ,
                left_table: left_hand.clone().table_name,
                left_column: left_hand.clone().name,
                right_hand: None,
                scholar: Some(Field::set_i64(300)),
            })),
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::GT,
                left_table: left_hand.clone().table_name,
                left_column: left_hand.clone().name,
                right_hand: None,
                scholar: Some(Field::set_i64(200)),
            })),
        );

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(300),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(or.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let truthy_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(250),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(or.clone(), &truthy_tuple, &gen_columns()),
            true
        );

        let falsey_tuple: Tuple = Tuple::new(vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(200),
            Field::set_i64(300),
        ]);
        assert_eq!(
            eval_selectors(or.clone(), &falsey_tuple, &gen_columns()),
            false
        );
    }
}
