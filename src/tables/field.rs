use std::string::String;
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;
use std::ops::Add;
use std::ops::Div;

use parser::token::Literal;

pub const INIT: usize = 0;
pub const KIND_BOOL: usize = 1;
pub const KIND_I64: usize = 2;
pub const KIND_U64: usize = 3;
pub const KIND_F64: usize = 4;
pub const KIND_STR: usize = 5;

#[derive(Debug, Clone)]
pub struct Field {
    pub kind: usize,
    pub b: Option<bool>,
    pub i: Option<i64>,
    pub u: Option<u64>,
    pub f: Option<String>,
    pub s: Option<String>,
}

impl Field {
    pub fn set_init() -> Field {
        Field {
            kind: INIT,
            b: None,
            i: None,
            u: None,
            f: None,
            s: None,
        }
    }

    pub fn set_bool(value: bool) -> Field {
        Field {
            kind: KIND_BOOL,
            b: Some(value),
            i: None,
            u: None,
            f: None,
            s: None,
        }
    }

    pub fn set_i64(value: i64) -> Field {
        Field {
            kind: KIND_I64,
            b: None,
            i: Some(value as i64),
            u: None,
            f: None,
            s: None,
        }
    }

    pub fn set_u64(value: u64) -> Field {
        Field {
            kind: KIND_U64,
            b: None,
            i: None,
            u: Some(value as u64),
            f: None,
            s: None,
        }
    }

    pub fn set_f64(value: f64) -> Field {
        Field {
            kind: KIND_F64,
            b: None,
            i: None,
            u: None,
            f: Some(value.to_string()),
            s: None,
        }
    }

    pub fn set_str(value: &str) -> Field {
        Field {
            kind: KIND_STR,
            b: None,
            i: None,
            u: None,
            f: None,
            s: Some(value.to_string()),
        }
    }

    pub fn set_same_type(&self, value: usize) -> Field {
        match self.kind {
            KIND_BOOL => {
                match value {
                    0 => Self::set_bool(false),
                    _ => Self::set_bool(true),
                }
            }
            KIND_I64 => {
                let converted: i64 = value as i64;
                Self::set_i64(converted)
            }
            KIND_U64 => {
                let converted: u64 = value as u64;
                Self::set_u64(converted)
            }
            KIND_F64 => {
                let converted: f64 = value as f64;
                Self::set_f64(converted)
            }
            KIND_STR => {
                let converted: &str = &*value.to_string();
                Self::set_str(converted)
            }
            _ => Self::set_init(),
        }
    }

    pub fn get_bool(&self) -> bool {
        self.b.unwrap()
    }

    pub fn get_i64(&self) -> i64 {
        self.i.unwrap()
    }

    pub fn get_u64(&self) -> u64 {
        self.u.unwrap()
    }

    // From outside, it behaves like f64
    pub fn get_f64(&self) -> f64 {
        self.f.clone().unwrap().parse::<f64>().unwrap()
    }

    // From inside, it behaves like String
    fn _get_f64(&self) -> String {
        self.f.clone().unwrap()
    }

    pub fn get_str(&self) -> String {
        self.s.clone().unwrap()
    }

    pub fn aggregatable(&self) -> bool {
        match self.kind {
            KIND_I64 => true,
            KIND_U64 => true,
            KIND_F64 => true,
            _ => false,
        }
    }

    pub fn to_string(&self) -> String {
        match self.kind {
            KIND_BOOL => self.get_bool().to_string(),
            KIND_I64 => self.get_i64().to_string(),
            KIND_U64 => self.get_u64().to_string(),
            KIND_F64 => self.get_f64().to_string(),
            KIND_STR => self.get_str(),
            _ => "".to_string(),
        }
    }

    pub fn print(&self) {
        match self.kind {
            KIND_BOOL => println!("{}", self.get_bool()),
            KIND_I64 => println!("{}", self.get_i64()),
            KIND_U64 => println!("{}", self.get_u64()),
            KIND_F64 => println!("{}", self.get_f64()),
            KIND_STR => println!("{}", self.get_str()),
            _ => println!("irregular data type"),
        }
    }
}

impl PartialEq for Field {
    fn eq(&self, other: &Field) -> bool {
        if self.kind != other.kind {
            return false;
        }

        match self.kind {
            INIT => self.kind == other.kind,
            KIND_BOOL => self.get_bool() == other.get_bool(),
            KIND_I64 => self.get_i64() == other.get_i64(),
            KIND_U64 => self.get_u64() == other.get_u64(),
            KIND_F64 => self._get_f64() == other._get_f64(),
            KIND_STR => self.get_str() == other.get_str(),
            _ => false,
        }
    }
}

impl Eq for Field {}

impl Hash for Field {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.kind {
            INIT => self.kind.hash(state),
            KIND_BOOL => self.get_bool().hash(state),
            KIND_I64 => self.get_i64().hash(state),
            KIND_U64 => self.get_u64().hash(state),
            KIND_F64 => self._get_f64().hash(state),
            KIND_STR => self.get_str().hash(state),
            _ => (),
        }
    }
}

impl PartialOrd for Field {
    fn partial_cmp(&self, other: &Field) -> Option<Ordering> {
        if self.kind != other.kind {
            return None;
        }

        match self.kind {
            KIND_BOOL => self.get_bool().partial_cmp(&other.get_bool()),
            KIND_I64 => self.get_i64().partial_cmp(&other.get_i64()),
            KIND_U64 => self.get_u64().partial_cmp(&other.get_u64()),
            KIND_F64 => self.get_f64().partial_cmp(&other.get_f64()),
            KIND_STR => self.get_str().partial_cmp(&other.get_str()),
            _ => None,
        }
    }
}

impl Add for Field {
    type Output = Field;
    fn add(self, other: Field) -> Field {
        if self.kind == INIT {
            return other;
        }

        if self.kind != other.kind {
            return self;
        }

        match self.kind {
            KIND_I64 => Self::set_i64(&self.get_i64() + &other.get_i64()),
            KIND_U64 => Self::set_u64(&self.get_u64() + &other.get_u64()),
            KIND_F64 => Self::set_f64(&self.get_f64() + &other.get_f64()),
            _ => self,
        }
    }
}

impl Div for Field {
    type Output = Field;
    fn div(self, other: Field) -> Field {
        if self.kind == INIT {
            return Self::set_init();
        }

        if self.kind != other.kind {
            return self;
        }

        match self.kind {
            KIND_I64 => Self::set_i64(&self.get_i64() / &other.get_i64()),
            KIND_U64 => Self::set_u64(&self.get_u64() / &other.get_u64()),
            KIND_F64 => Self::set_f64(&self.get_f64() / &other.get_f64()),
            _ => self,
        }
    }
}

impl From<Literal> for Field {
    fn from(lit: Literal) -> Field {
        match lit {
            Literal::Int(i) => Self::set_i64(i),
            Literal::Float(f) => Self::set_f64(f),
            Literal::String(s) => Self::set_str(&s),
            Literal::Bool(b) => match b {
                0 => Self::set_bool(false),
                _ => Self::set_bool(true),
            }
        }
    }
}

#[cfg(test)]
mod tests {}
