pub mod field {
    use std::string::String;

    pub const KIND_I64: usize = 0;
    pub const KIND_U64: usize = 1;
    pub const KIND_F64: usize = 2;
    pub const KIND_STR: usize = 3;

    #[derive(Clone)]
    pub struct Field {
        pub kind: usize,
        pub i: Option<i64>,
        pub u: Option<u64>,
        pub f: Option<f64>,
        pub s: Option<String>,
    }

    impl Field {
        pub fn set_i64(value: i64) -> Field {
            Field {
                kind: KIND_I64,
                i: Some(value as i64),
                u: None,
                f: None,
                s: None,
            }
        }

        pub fn set_u64(value: u64) -> Field {
            Field {
                kind: KIND_U64,
                i: None,
                u: Some(value as u64),
                f: None,
                s: None,
            }
        }

        pub fn set_f64(value: f64) -> Field {
            Field {
                kind: KIND_F64,
                i: None,
                u: None,
                f: Some(value as f64),
                s: None,
            }
        }

        pub fn set_str(value: &str) -> Field {
            Field {
                kind: KIND_STR,
                i: None,
                u: None,
                f: None,
                s: Some(value.to_string()),
            }
        }

        pub fn get_i64(&self) -> i64 {
            self.i.unwrap()
        }

        pub fn get_u64(&self) -> u64 {
            self.u.unwrap()
        }

        pub fn get_f64(&self) -> f64 {
            self.f.unwrap()
        }

        pub fn get_str(&self) -> String {
            self.s.clone().unwrap()
        }
    }
}

