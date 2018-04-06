use bincode::{serialize, deserialize};

use Field;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

    pub fn encode(&self) -> Result<Vec<u8>, BinaryError> {
        match serialize(&self) {
            Ok(bin) => Ok(bin),
            _ => Err(BinaryError::EncodeError),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Tuple, BinaryError> {
        match deserialize(bytes) {
            Ok(tuple) => Ok(tuple),
            _ => Err(BinaryError::DecodeError),
        }
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

#[derive(Debug)]
pub enum BinaryError {
    EncodeError,
    DecodeError,
}

