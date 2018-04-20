use super::*;

mod selectors {
    #[test]
    fn create_equal() {
        let left: Target = Target {
            table_name: Some("shohin"),
            name: "shohin_name"
        };

        let right_column: Target = Target {
            table_name: Some("shohin"),
            name: "shohin_id"
        };

        let selector_column: Equal = Box::new(Equal {
            left_table: Some("shohin"),
            left_column: "shohin_name",
            right_hand: Some(Target {table_name: Some("shohin")}),
            scholar: None,
        };,

        assert_eq!(selector_column, Equal::new(left, Some(right_column), None));

        let right_literal: Lit = Lit(String("apple"));

        let selector_scholar: Equal = Box::new(Equal {
            left_table: Some("shohin"),
            left_column: "shohin_name",
            right_hand: None,
            scholar: Field::set_str("apple"),
        };

        assert_eq!(selector_scholar, Equal::new(left, None, Some(right_literal)));
    }
}
