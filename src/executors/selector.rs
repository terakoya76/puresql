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
            ("kubun_id".to_owned(), DataType::Int),
            ("price".to_owned(), DataType::Int),
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

    fn gen_tuple() -> Tuple {
        let fields: Vec<Field> = vec![
            Field::set_i64(1),
            Field::set_str("apple"),
            Field::set_i64(1),
            Field::set_i64(300),
        ];

        Tuple::new(fields)
    }

    #[test]
    fn test_find_field() {
        let field = find_field(
            &gen_tuple(),
            &gen_columns(),
            Some("shohin".to_owned()),
            "price".to_owned(),
        );
        assert_eq!(field, Ok(Field::set_i64(300)));

        let field = find_field(&gen_tuple(), &gen_columns(), None, "price".to_owned());
        assert_eq!(field, Ok(Field::set_i64(300)));

        let field = find_field(
            &gen_tuple(),
            &gen_columns(),
            None,
            "fictional_column".to_owned(),
        );
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
        let selectors = build_selectors(column_condition).unwrap();
        let equ = Selectors::Leaf(Selector {
            kind: Operator::Equ,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });
        assert_eq!(selectors, equ);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            true
        );

        // build w/ scholar right hand
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::Equ,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition).unwrap();
        let equ = Selectors::Leaf(Selector {
            kind: Operator::Equ,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_str("apple")),
        });
        assert_eq!(selectors, equ);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            true
        );
    }

    #[test]
    fn build_not_equal_leaf() {
        let left_hand: Target = gen_left_hand4str();
        let right_column: Target = Target {
            table_name: Some("shohin".to_owned()),
            name: "shohin_id".to_owned(),
        };
        let right_literal: Literal = Literal::String("grape".to_owned());

        // build w/ column right hand
        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::NEqu,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition).unwrap();
        let nequ = Selectors::Leaf(Selector {
            kind: Operator::NEqu,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });
        assert_eq!(selectors, nequ);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            true
        );

        // build w/ scholar right hand
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::NEqu,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition).unwrap();
        let nequ = Selectors::Leaf(Selector {
            kind: Operator::NEqu,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_str("grape")),
        });
        assert_eq!(selectors, nequ);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            true
        );
    }

    #[test]
    fn build_gt_leaf() {
        let left_hand: Target = gen_left_hand4int();
        let right_column: Target = gen_right_column4int();
        let right_literal: Literal = Literal::Int(200);

        // build w/ column right hand
        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GT,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition).unwrap();
        let gt = Selectors::Leaf(Selector {
            kind: Operator::GT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });
        assert_eq!(selectors, gt);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            false
        );

        // build w/ scholar right hand
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GT,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition).unwrap();
        let gt = Selectors::Leaf(Selector {
            kind: Operator::GT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(200)),
        });
        assert_eq!(selectors, gt);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            true
        );
    }

    #[test]
    fn build_lt_leaf() {
        let left_hand: Target = gen_left_hand4int();
        let right_column: Target = gen_right_column4int();
        let right_literal: Literal = Literal::Int(400);

        // build w/ column right hand
        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LT,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition).unwrap();
        let lt = Selectors::Leaf(Selector {
            kind: Operator::LT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });
        assert_eq!(selectors, lt);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            false
        );

        // build w/ scholar right hand
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LT,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition).unwrap();
        let lt = Selectors::Leaf(Selector {
            kind: Operator::LT,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(400)),
        });
        assert_eq!(selectors, lt);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            true
        );
    }

    #[test]
    fn build_ge_leaf() {
        let left_hand: Target = gen_left_hand4int();
        let right_column: Target = gen_right_column4int();
        let right_literal: Literal = Literal::Int(200);

        // build w/ column right hand
        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GE,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition).unwrap();
        let ge = Selectors::Leaf(Selector {
            kind: Operator::GE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });
        assert_eq!(selectors, ge);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            true
        );

        // build w/ scholar right hand
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::GE,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition).unwrap();
        let ge = Selectors::Leaf(Selector {
            kind: Operator::GE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(200)),
        });
        assert_eq!(selectors, ge);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            true
        );
    }

    #[test]
    fn build_le_leaf() {
        let left_hand: Target = gen_left_hand4int();
        let right_column: Target = gen_right_column4int();
        let right_literal: Literal = Literal::Int(400);

        // build w/ column right hand
        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LE,
            right: Comparable::Target(right_column.clone()),
        });
        let selectors = build_selectors(column_condition).unwrap();
        let le = Selectors::Leaf(Selector {
            kind: Operator::LE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: Some(right_column),
            scholar: None,
        });
        assert_eq!(selectors, le);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            true
        );

        // build w/ scholar right hand
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand.clone(),
            op: Operator::LE,
            right: Comparable::Lit(right_literal),
        });
        let selectors = build_selectors(scholar_condition).unwrap();
        let le = Selectors::Leaf(Selector {
            kind: Operator::LE,
            left_table: left_hand.clone().table_name,
            left_column: left_hand.clone().name,
            right_hand: None,
            scholar: Some(Field::set_i64(400)),
        });
        assert_eq!(selectors, le);
        assert_eq!(
            eval_selectors(selectors, &gen_tuple(), &gen_columns()),
            true
        );
    }

    #[test]
    fn build_and_node() {
        let left_hand_str: Target = gen_left_hand4str();
        let right_column_str: Target = gen_right_column4str();

        let left_hand_int: Target = gen_left_hand4int();
        let right_literal_int: Literal = Literal::Int(200);

        // condition whose result is true
        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand_str.clone(),
            op: Operator::Equ,
            right: Comparable::Target(right_column_str.clone()),
        });

        // condition whose result is false
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand_int.clone(),
            op: Operator::GT,
            right: Comparable::Lit(right_literal_int),
        });

        let condition: Conditions =
            Conditions::And(Box::new(column_condition), Box::new(scholar_condition));
        let built_selectors = build_selectors(condition).unwrap();
        let selectors = Selectors::And(
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::Equ,
                left_table: left_hand_str.clone().table_name,
                left_column: left_hand_str.clone().name,
                right_hand: Some(right_column_str),
                scholar: None,
            })),
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::GT,
                left_table: left_hand_int.clone().table_name,
                left_column: left_hand_int.clone().name,
                right_hand: None,
                scholar: Some(Field::set_i64(200)),
            })),
        );
        assert_eq!(built_selectors, selectors);
        assert_eq!(
            eval_selectors(built_selectors, &gen_tuple(), &gen_columns()),
            true
        );
    }

    #[test]
    fn build_or_node() {
        let left_hand_str: Target = gen_left_hand4str();
        let right_column_str: Target = gen_right_column4str();

        let left_hand_int: Target = gen_left_hand4int();
        let right_literal_int: Literal = Literal::Int(400);

        // condition whose result is true
        let column_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand_str.clone(),
            op: Operator::Equ,
            right: Comparable::Target(right_column_str.clone()),
        });

        // condition whose result is false
        let scholar_condition: Conditions = Conditions::Leaf(Condition {
            left: left_hand_int.clone(),
            op: Operator::GT,
            right: Comparable::Lit(right_literal_int),
        });

        let condition: Conditions =
            Conditions::Or(Box::new(column_condition), Box::new(scholar_condition));
        let built_selectors = build_selectors(condition).unwrap();
        let selectors = Selectors::Or(
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::Equ,
                left_table: left_hand_str.clone().table_name,
                left_column: left_hand_str.clone().name,
                right_hand: Some(right_column_str),
                scholar: None,
            })),
            Box::new(Selectors::Leaf(Selector {
                kind: Operator::GT,
                left_table: left_hand_int.clone().table_name,
                left_column: left_hand_int.clone().name,
                right_hand: None,
                scholar: Some(Field::set_i64(400)),
            })),
        );
        assert_eq!(built_selectors, selectors);
        assert_eq!(
            eval_selectors(built_selectors, &gen_tuple(), &gen_columns()),
            true
        );
    }
}
