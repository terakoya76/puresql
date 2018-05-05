use parser::token::Token;
use parser::position::Position;

#[derive(Debug, Clone, PartialEq)]
pub struct TokenPos {
    pub token: Token,
    pub pos: Position,
}

#[cfg(test)]
mod tests {}
