use Field;

#[derive(Debug, Clone)]
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
            buffer += &f.to_string();
        }
        println!("{}", buffer);
        buffer.clear();
    }
}

