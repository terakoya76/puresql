pub mod field {
    use std::string::String;
    use std::cmp::Ordering;

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

    impl PartialEq for Field {
        fn eq(&self, other: &Field) -> bool {
            if self.kind != other.kind {
                return false;
            }

            match self.kind {
                KIND_I64 => self.get_i64() == other.get_i64(),
                KIND_U64 => self.get_u64() == other.get_u64(),
                KIND_F64 => self.get_f64() == other.get_f64(),
                KIND_STR => self.get_str() == other.get_str(),
                _ => false,
            }
        }
    }

    impl PartialOrd for Field {
        fn partial_cmp(&self, other: &Field) -> Option<Ordering> {
            if self.kind != other.kind {
                return None;
            }

            match self.kind {
                KIND_I64 => self.get_i64().partial_cmp(&other.get_i64()),
                KIND_U64 => self.get_u64().partial_cmp(&other.get_u64()),
                KIND_F64 => self.get_f64().partial_cmp(&other.get_f64()),
                KIND_STR => self.get_str().partial_cmp(&other.get_str()),
                _ => None,
            }
        }
    }
}

