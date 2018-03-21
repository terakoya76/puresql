use parser::token::Token;
use parser::span::Span;

#[derive(Debug)]
pub struct TokenSpan {
    pub token: Token,
    pub span: Span,
}

