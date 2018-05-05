use std::str::Chars;

use parser::token::{Literal, Token};
use parser::position::Position;
use parser::token_pos::TokenPos;

#[derive(Debug, Clone)]
pub struct Lexer<'c> {
    chars: Chars<'c>,
    last_char: Option<char>,
    curr_char: Option<char>,
    next_char: Option<char>,
    last_pos: Option<usize>,
    curr_pos: Option<usize>,
    span_start: Option<usize>,
}

impl<'c> Lexer<'c> {
    pub fn new(query: &'c str) -> Lexer<'c> {
        let mut lexer: Lexer = Lexer {
            chars: query.chars(),
            last_char: None,
            curr_char: None,
            next_char: None,
            last_pos: None,
            curr_pos: None,
            span_start: None,
        };
        lexer.double_bump();
        lexer
    }

    pub fn bump(&mut self) {
        self.last_char = self.curr_char;
        self.curr_char = self.next_char;
        self.next_char = self.chars.next();

        self.last_pos = self.curr_pos;

        match self.next_char {
            None => {}
            Some(c) => {
                self.curr_pos = match self.curr_pos {
                    Some(n) => Some(n + c.len_utf8()),
                    None => Some(c.len_utf8()),
                }
            }
        };
    }

    pub fn double_bump(&mut self) {
        self.bump();
        self.bump();
    }

    pub fn scan_words(&mut self) -> String {
        let mut s: String = String::new();
        loop {
            match self.curr_char.unwrap_or(' ') {
                c @ 'a'...'z' | c @ 'A'...'Z' | c @ '0'...'9' | c @ '_' => {
                    s.push(c);
                }
                _ => break,
            }
            self.bump();
        }
        s
    }

    pub fn scan_nums(&mut self) -> String {
        let mut s: String = String::new();
        loop {
            match self.curr_char.unwrap_or(' ') {
                c @ '0'...'9' | c @ '.' => {
                    s.push(c);
                }
                _ => break,
            }
            self.bump();
        }
        s
    }

    pub fn scan_literal(&mut self) -> Result<String, LexError> {
        let mut l: String = String::new();
        self.bump();

        loop {
            match self.curr_char {
                None => return Err(LexError::UnclosedQuationmark),
                Some(c) => match c {
                    '\'' | '"' => break,
                    _ => l.push(c),
                },
            }
            self.bump();
        }
        self.bump();
        Ok(l)
    }

    pub fn next_parsable_token(&mut self) -> Result<Option<TokenPos>, LexError> {
        let token_pos: Option<TokenPos> = try!(self.next());
        let is_ws: bool = match token_pos {
            None => false,
            Some(ref ts) => match ts.token {
                Token::WS => true,
                _ => false,
            },
        };

        if is_ws {
            self.next()
        } else {
            Ok(token_pos)
        }
    }

    pub fn skip_whitespace(&mut self) {
        while is_whitespace(self.curr_char.unwrap_or(' ')) {
            self.bump();
        }
    }

    pub fn next(&mut self) -> Result<Option<TokenPos>, LexError> {
        let next_char = self.next_char.unwrap_or('\x00');

        self.span_start = self.curr_pos;

        let curr_char = match self.curr_char {
            None => return Ok(None),
            Some(c) => c,
        };

        let token = match curr_char {
            'a'...'z' | 'A'...'Z' => {
                let w = self.scan_words();
                Token::Word(w)
            }

            '0'...'9' => {
                let n = self.scan_nums();
                if let Ok(i) = n.parse::<i64>() {
                    Token::Lit(Literal::Int(i))
                } else {
                    if let Ok(f) = n.parse::<f64>() {
                        Token::Lit(Literal::Float(f))
                    } else {
                        Token::Unknown
                    }
                }
            }

            ';' => {
                self.bump();
                Token::Semi
            }

            '.' => {
                self.bump();
                Token::Dot
            }

            ',' => {
                self.bump();
                Token::Comma
            }

            '(' => {
                self.bump();
                Token::OpPar
            }

            ')' => {
                self.bump();
                Token::ClPar
            }

            '\'' | '"' => {
                let l = try!(self.scan_literal());
                Token::Lit(Literal::String(l))
            }

            '=' => {
                self.bump();
                Token::Equ
            }

            '>' if next_char == '=' => {
                self.double_bump();
                Token::GE
            }

            '>' => {
                self.bump();
                Token::GT
            }

            '<' if next_char == '=' => {
                self.double_bump();
                Token::LE
            }

            '<' if next_char == '>' => {
                self.double_bump();
                Token::NEqu
            }

            '<' => {
                self.bump();
                Token::LT
            }

            '+' => {
                self.bump();
                Token::Add
            }

            '-' => {
                self.bump();
                Token::Sub
            }

            '/' => {
                self.bump();
                Token::Div
            }

            '%' => {
                self.bump();
                Token::Mod
            }

            '*' => {
                self.bump();
                Token::Star
            }

            c if is_whitespace(c) => {
                self.skip_whitespace();
                Token::WS
            }

            _ => {
                self.bump();
                Token::Unknown
            }
        };

        Ok(Some(TokenPos {
            token: token,
            pos: Position {
                start: self.span_start.unwrap(),
                end: self.curr_pos.unwrap(),
            },
        }))
    }
}

fn is_whitespace(c: char) -> bool {
    match c {
        ' ' | '\n' | '\t' => true,
        _ => false,
    }
}

#[derive(Debug, PartialEq)]
pub enum LexError {
    UnclosedQuationmark,
}
