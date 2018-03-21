#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Word(String),
    Lit(Literal),
    Semi,
    Dot,
    Comma,
    OpPar,
    ClPar,
    ADel,
    Equ,
    GT,
    LT,
    GE,
    LE,
    NEqu,
    Add,
    Sub,
    Div,
    Mod,
    Star,
    WS,
    Unknown,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Int(i64),
    Float(f64),
    Bool(u8),
}

/*
impl Literal {
    pub fn into_data_src(&self) -> DataSrc {
        match self {
            &Literal::String(ref s) => DataSrc::String(s.clone()),
            &Literal::Int(ref i) => DataSrc::Int(i.clone()),
            &Literal::Float(ref f) => DataSrc::String(f.clone()),
            &Literal::Bool(ref b) => DataSrc::Bool(b.clone()),
        }
    }
}
*/

