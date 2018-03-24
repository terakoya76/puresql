use std::mem::swap;

use data_type::DataType;
use parser::token::{Token, Literal};
use parser::span::Span;
use parser::token_span::TokenSpan;
use parser::lexer::{Lexer, LexError};
use parser::keyword::Keyword;
use parser::statement::*;

#[derive(Debug)]
pub struct Parser<'c> {
    lexer: Lexer<'c>,
    last_token: Option<TokenSpan>,
    curr_token: Option<TokenSpan>,
    next_token: Option<TokenSpan>,
}

impl<'c> Parser<'c> {
    pub fn new(query: &'c str) -> Parser<'c> {
        let lexer: Lexer = Lexer::new(query);
        let mut parser: Parser = Parser {
            lexer: lexer,
            last_token: None,
            curr_token: None,
            next_token: None,
        };
        parser.double_bump();
        parser
    }

    pub fn bump(&mut self) -> Result<(), ParseError> {
        swap(&mut self.last_token, &mut self.curr_token);
        swap(&mut self.curr_token, &mut self.next_token);
        self.next_token = try!(self.lexer.next_parsable_token());
        Ok(())
    }

    pub fn double_bump(&mut self) {
        self.bump();
        self.bump();
    }

    pub fn build_ast(&mut self, stmt: Statement) -> Result<Statement, ParseError> {
        try!(self.bump());
        if self.curr_token.is_none() {
            Ok(stmt)
        } else {
            Err(ParseError::InvalidEoq)
        }
    }

    pub fn validate_datatype(&mut self) -> Result<DataType, ParseError> {
        let mut span_start;
        let mut span_end;
        let mut datatype;

        {
            let token_span: &TokenSpan = match self.curr_token {
                None => return Err(ParseError::UnexepectedEoq),
                Some(ref ts) => ts,
            };

            span_start = token_span.span.start;
            span_end = token_span.span.end;

            let word: &str = match token_span.token {
                Token::Word(ref s) => s,
                _ => return Err(ParseError::UndefinedDatatype(Span {
                    start: span_start,
                    end: span_end,
                })),
            };
            datatype = word.to_lowercase();
        }

        let found_datatype: DataType = match &datatype[..] {
            "int" => DataType::Int,
            "bool" => DataType::Bool,
            "char" => {
                try!(self.bump());
                try!(self.validate_token(&[Token::OpPar]));
                try!(self.bump());
                let length = try!(self.validate_number());
                try!(self.bump());
                try!(self.validate_token(&[Token::ClPar]));

                let l = match length {
                    Literal::Int(i) => {
                        if 0 <= i && i <= (u8::max_value() as i64) {
                            i as u8
                        } else {
                            return Err(ParseError::UnexpectedDatatype(Span {
                                start: span_start,
                                end: span_end,
                            }));
                        }
                    },
                    _ => return Err(ParseError::UnexpectedDatatype(Span {
                        start: span_start,
                        end: span_end,
                    })),
                };
                DataType::Char(l)
            },
            _ => return Err(ParseError::UndefinedDatatype(Span {
                start: span_start,
                end: span_end,
            })),
        };
        Ok(found_datatype)
    }

    pub fn validate_number(&self) -> Result<Literal, ParseError> {
        let token_span: &TokenSpan = match self.curr_token {
            None => return Err(ParseError::UnexepectedEoq),
            Some(ref ts) => ts,
        };

        let span_start: usize = token_span.span.start;
        let span_end: usize = token_span.span.end;

        let found_number: Literal = match token_span.token {
            Token::Lit(Literal::Int(s)) => Literal::Int(s),
            Token::Lit(Literal::Float(s)) => Literal::Float(s),
            _ => return Err(ParseError::UndefinedNumber(Span {
                start: span_start,
                end: span_end,
            })),
        };

        Ok(found_number)
    }

    pub fn validate_word(&self, allow_keyword: bool) -> Result<String, ParseError> {
        let token_span: &TokenSpan = match self.curr_token {
            None => return Err(ParseError::UnexepectedEoq),
            Some(ref token_span) => token_span,
        };

        let span_start: usize = token_span.span.start;
        let span_end: usize = token_span.span.end;

        let found_word: &str = match token_span.token {
            Token::Word(ref s) => s,
            _ => return Err(ParseError::UnexpectedToken(Span {
                start: span_start,
                end: span_end,
            })),
        };

        if keyword_from_str(&found_word).is_some() && !allow_keyword {
            Err(ParseError::ReservedKeyword(Span {
                start: span_start,
                end: span_end,
            }))
        } else {
            Ok(found_word.to_string())
        }
    }

    pub fn validate_column_info(&mut self) -> Result<ColumnInfo, ParseError> {
        let name: String = try!(self.validate_word(true));
        try!(self.bump());
        let dtype: DataType = try!(self.validate_datatype());

        while self.next_token.is_some() && !self.check_next_token(&[Token::ClPar, Token::Comma]) {
            //try!(self.bump());
        }

        Ok(ColumnInfo {
            name: name,
            datatype: dtype,
        })
    }

    pub fn validate_token(&self, expected_tokens: &[Token]) -> Result<Token, ParseError> {
        let token_span: &TokenSpan = match self.curr_token {
            None => return Err(ParseError::UnexepectedEoq),
            Some(ref token_span) => token_span,
        };

        if expected_tokens.contains(&(token_span.token)) {
            Ok(token_span.token.clone())
        } else {
            Err(ParseError::UnexpectedToken(Span {
                start: token_span.span.start,
                end: token_span.span.end,
            }))
        }
    }

    pub fn check_next_token(&self, expected_tokens: &[Token]) -> bool {
        match self.next_token {
            None => false,
            Some(ref token) => expected_tokens.contains(&token.token),
        }
    }

    pub fn validate_keyword(&self, expected_keywords: &[Keyword]) -> Result<Keyword, ParseError> {
        let token_span: &TokenSpan = match self.curr_token {
            None => return Err(ParseError::UnexepectedEoq),
            Some(ref ts) => ts,
        };

        let span_start: usize = token_span.span.start;
        let span_end: usize = token_span.span.end;

        let word: &str = match token_span.token {
            Token::Word(ref s) => s,
            _ => return Err(ParseError::UnexpectedToken(Span {
                start: span_start,
                end: span_end,
            })),
        };

        let found_keyword: Keyword = match keyword_from_str(&word) {
            None => return Err(ParseError::UndefinedKeyword(Span {
                start: span_start,
                end: span_end,
            })),
            Some(keyword) => keyword,
        };

        if expected_keywords.contains(&found_keyword) {
            Ok(found_keyword)
        } else {
            Err(ParseError::UnexpectedKeyword(Span {
                start: span_start,
                end: span_end,
            }))
        }
    }

    pub fn check_next_keyword(&self, expected_keywords: &[Keyword]) -> bool {
        let token_span: &TokenSpan = match self.next_token {
            None => return false,
            Some(ref ts) => ts,
        };

        let keyword_opt: &str = match token_span.token {
            Token::Word(ref s) => s,
            _ => return false,
        };

        match keyword_from_str(&keyword_opt) {
            None => return false,
            Some(keyword) => expected_keywords.contains(&keyword),
        }
    }

    pub fn parse(&mut self) -> Result<Statement, ParseError> {
        let starting_keywords: &[Keyword] = starting_keywords();
        match try!(self.validate_keyword(starting_keywords)) {
            Keyword::Create => {
                let stmt: Statement = Statement::DDL(DDL::Create(try!(self.parse_create_stmt())));
                Ok(try!(self.build_ast(stmt)))
            },
            /*
            Keyword::Drop => {
                let stmt: Statement = Statement::DDL(DDL::Drop(try!(self.parse_drop_stmt())));
                Ok(try!(self.build_ast(stmt)))
            },
            */
            /*
            Keyword::Alter => {
                let stmt: Statement = Statement::DDL(DDL::Alter(try!(self.parse_alter_stmt())));
                Ok(try!(self.build_ast(stmt)))
            },
            */
            /*
            Keyword::Use => {
                let stmt: Statement = Statement::DML(DML::Use(try!(self.parse_use_stmt())));
                Ok(try!(self.build_ast(stmt)))
            },
            */
            /*
            Keyword::Describe => {
                let stmt: Statement = Statement::DML(DML::Use(try!(self.parse_describe_stmt())));
                Ok(try!(self.build_ast(stmt)))
            },
            */
            /*
            Keyword::Select => {
                let stmt: Statement = Statement::DML(DML::Select(try!(self.parse_select_stmt())));
                Ok(try!(self.build_ast(stmt)))
            },
            */
            /*
            Keyword::Update => {
                let stmt: Statement = Statement::DML(DML::Update(try!(self.parse_update_stmt())));
                Ok(try!(self.build_ast(stmt)))
            },
            */
            /*
            Keyword::Insert => {
                let stmt: Statement = Statement::DML(DML::Insert(try!(self.parse_insert_stmt())));
                Ok(try!(self.build_ast(stmt)))
            },
            */
            /*
            Keyword::Delete => {
                let stmt: Statement = Statement::DML(DML::Delete(try!(self.parse_delete_stmt())));
                Ok(try!(self.build_ast(stmt)))
            },
            */
            _ => Err(ParseError::UndefinedStatementError),
        }
    }

    pub fn parse_create_stmt(&mut self) -> Result<CreateStmt, ParseError> {
        try!(self.bump());
        match try!(self.validate_keyword(&[Keyword::Table])) {
            Keyword::Table => Ok(CreateStmt::Table(try!(self.parse_create_table_stmt()))),
            _ => Err(ParseError::UndefinedStatementError),
        }
    }

    pub fn parse_create_table_stmt(&mut self) -> Result<CreateTableStmt, ParseError> {
        try!(self.bump());
        let mut stmt: CreateTableStmt = CreateTableStmt {
            table_name: try!(self.validate_word(false)),
            columns: Vec::new(),
        };

        try!(self.bump());
        if self.curr_token.is_none() {
            return Ok(stmt);
        }

        try!(self.validate_token(&[Token::OpPar]));
        stmt.columns = try!(self.parse_create_columns());
        Ok(stmt)
    }

    pub fn parse_create_columns(&mut self) -> Result<Vec<ColumnInfo>, ParseError> {
        try!(self.bump());
        let mut columns: Vec<ColumnInfo> = Vec::new();

        while !self.validate_token(&[Token::ClPar]).is_ok() {
            columns.push(try!(self.validate_column_info()));
            try!(self.bump());
            match try!(self.validate_token(&[Token::Comma, Token::ClPar])) {
                Token::Comma => try!(self.bump()),
                _ => (),
            }
        }
        Ok(columns)
    }
}

fn starting_keywords() -> &'static [Keyword] {
    &[
        Keyword::Create,
        //Keyword::Drop,
        //Keyword::Alter,
        //Keyword::Use,
        //Keyword::Describe,
        Keyword::Select,
        Keyword::Update,
        Keyword::Insert,
        Keyword::Delete,
    ]
}

fn keyword_from_str(string: &str) -> Option<Keyword> {
    let lower_string: String = string.to_lowercase();
    match &lower_string[..] {
        "create" => Some(Keyword::Create),
        //"drop" => Some(Keyword::Drop),
        //"alter" => Some(Keyword::Alter),
        //"use" => Some(Keyword::Use),
        //"describe" => Some(Keyword::Describe),
        "select" => Some(Keyword::Select),
        "update" => Some(Keyword::Update),
        "insert" => Some(Keyword::Insert),
        "delete" => Some(Keyword::Delete),
        "set" => Some(Keyword::Set),
        "table" => Some(Keyword::Table),
        //"database" => Some(Keyword::Database),
        //"view" => Some(Keyword::View),
        "column" => Some(Keyword::Column),
        "from" => Some(Keyword::From),
        "where" => Some(Keyword::Where),
        "group" => Some(Keyword::Group),
        "order" => Some(Keyword::Order),
        "having" => Some(Keyword::Having),
        "limit" => Some(Keyword::Limit),
        //"modify" => Some(Keyword:::Modify),
        "add" => Some(Keyword::Add),
        "into" => Some(Keyword::Into),
        "values" => Some(Keyword::Values),
        "and" => Some(Keyword::And),
        "or" => Some(Keyword::Or),
        "as" => Some(Keyword::As),
        "by" => Some(Keyword::By),
        "asc" => Some(Keyword::Asc),
        "desc" => Some(Keyword::Desc),
        //"primary" => Some(Keyword::Primary),
        //"key" => Some(Keyword::Key),
        //"replace" => Some(Keyword::Replace),
        //"auto_increment" => Some(Keyword::AutoIncrement),
        //"comment" => Some(Keyword::Comment),
        _ => None,
    }
}

#[derive(Debug, PartialEq)]
pub enum ParseError {
    LexError(LexError),
    ReservedKeyword(Span),
    InvalidEoq,
    UndefinedStatementError,
    UndefinedKeyword(Span),
    UndefinedDatatype(Span),
    UndefinedNumber(Span),
    UnexepectedEoq,
    UnexpectedKeyword(Span),
    UnexpectedDatatype(Span),
    UnexpectedToken(Span),
}

impl From<LexError> for ParseError {
    fn from(err: LexError) -> ParseError {
        ParseError::LexError(err)
    }
}

