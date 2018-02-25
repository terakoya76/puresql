pub mod tuple {
    use field::field::Field;

    #[derive(Clone)]
    pub struct Tuple {
        pub fields: Vec<Field>,
    }

    impl Tuple {
        pub fn new(fields: Vec<Field>) -> Tuple {
            Tuple {
                fields: fields,
            }
        }

        pub fn append(&self, tuple: &Tuple) -> Tuple {
            let mut fields: Vec<Field> = self.fields.clone();
            fields.append(&mut tuple.fields.clone());
            Tuple::new(fields)
        }

        pub fn to_string(&self) {
            let mut buffer: String = String::new();
            for f in &self.fields {
                buffer += "|";
                match f.kind {
                    ::field::field::KIND_I64 => buffer += &f.get_i64().to_string(),
                    ::field::field::KIND_U64 => buffer += &f.get_u64().to_string(),
                    ::field::field::KIND_F64 => buffer += &f.get_f64().to_string(),
                    ::field::field::KIND_STR => buffer += &f.get_str(),
                    _ => buffer += "Unsupported Data Type",
                }
            }
            println!("{}", buffer);
            buffer.clear();
        }
    }
}

