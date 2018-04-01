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

