use Field;

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

    pub fn print(&self) {
        let mut buffer: String = String::new();
        for f in &self.fields {
            buffer += "|";
            match f.kind {
                KIND_I64 => buffer += &f.to_string(),
                KIND_U64 => buffer += &f.to_string(),
                KIND_F64 => buffer += &f.to_string(),
                KIND_STR => buffer += &f.to_string(),
                _ => buffer += "Unsupported Data Type",
            }
        }
        println!("{}", buffer);
        buffer.clear();
    }
}
