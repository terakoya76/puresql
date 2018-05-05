use std::mem::swap;

use data_type::DataType;
use parser::token::{Literal, Token};
use parser::token_pos::TokenPos;
use parser::lexer::{LexError, Lexer};
use parser::keyword::Keyword;
use parser::statement::*;

#[derive(Debug)]
pub struct Parser<'c> {
    lexer: Lexer<'c>,
    last_token: Option<TokenPos>,
    curr_token: Option<TokenPos>,
    next_token: Option<TokenPos>,
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
        let _ = parser.double_bump();
        parser
    }

    pub fn bump(&mut self) -> Result<(), ParseError> {
        swap(&mut self.last_token, &mut self.curr_token);
        swap(&mut self.curr_token, &mut self.next_token);
        self.next_token = try!(self.lexer.next_parsable_token());
        Ok(())
    }

    pub fn double_bump(&mut self) -> Result<(), ParseError> {
        let _ = self.bump();
        let _ = self.bump();
        Ok(())
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
        let debug_token_pos: TokenPos;
        let datatype: String;

        {
            let token_pos: &TokenPos = match self.curr_token {
                None => return Err(ParseError::UnexepectedEoq),
                Some(ref ts) => ts,
            };
            debug_token_pos = token_pos.clone();

            let word: &str = match token_pos.token {
                Token::Word(ref s) => s,
                _ => return Err(ParseError::UndefinedDatatype(debug_token_pos)),
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
                            return Err(ParseError::UnexpectedDatatype(debug_token_pos));
                        }
                    }
                    _ => return Err(ParseError::UnexpectedDatatype(debug_token_pos)),
                };
                DataType::Char(l)
            }
            _ => return Err(ParseError::UndefinedDatatype(debug_token_pos)),
        };
        Ok(found_datatype)
    }

    pub fn validate_word(&self, allow_keyword: bool) -> Result<String, ParseError> {
        let token_pos: &TokenPos = match self.curr_token {
            None => return Err(ParseError::UnexepectedEoq),
            Some(ref token_pos) => token_pos,
        };

        let found_word: &str = match token_pos.token {
            Token::Word(ref s) => s,
            _ => return Err(ParseError::UnexpectedToken(token_pos.clone())),
        };

        if keyword_from_str(&found_word).is_some() && !allow_keyword {
            Err(ParseError::ReservedKeyword(token_pos.clone()))
        } else {
            Ok(found_word.to_string())
        }
    }

    pub fn validate_number(&self) -> Result<Literal, ParseError> {
        let token_pos: &TokenPos = match self.curr_token {
            None => return Err(ParseError::UnexepectedEoq),
            Some(ref ts) => ts,
        };

        let found_number: Literal = match token_pos.token {
            Token::Lit(Literal::Int(s)) => Literal::Int(s),
            Token::Lit(Literal::Float(s)) => Literal::Float(s),
            _ => return Err(ParseError::UndefinedNumber(token_pos.clone())),
        };

        Ok(found_number)
    }

    pub fn validate_literal(&self) -> Result<Literal, ParseError> {
        let token_pos: &TokenPos = match self.curr_token {
            None => return Err(ParseError::UnexepectedEoq),
            Some(ref ts) => ts,
        };

        let found_lit: Literal = match token_pos.token {
            Token::Word(ref s) => {
                let lower_str: String = s.to_lowercase();
                match &lower_str[..] {
                    "true" => Literal::Bool(1),
                    "false" => Literal::Bool(0),
                    _ => return Err(ParseError::UnexpectedToken(token_pos.clone())),
                }
            }
            Token::Lit(ref l) => l.clone(),
            _ => return Err(ParseError::UnexpectedToken(token_pos.clone())),
        };

        Ok(found_lit)
    }

    pub fn validate_column_def(&mut self) -> Result<ColumnDef, ParseError> {
        let name: String = try!(self.validate_word(true));
        try!(self.bump());
        let dtype: DataType = try!(self.validate_datatype());

        while self.next_token.is_some() && !self.check_next_token(&[Token::ClPar, Token::Comma]) {
            //try!(self.bump());
        }

        Ok(ColumnDef {
            name: name,
            datatype: dtype,
        })
    }

    pub fn validate_token(&self, expected_tokens: &[Token]) -> Result<Token, ParseError> {
        let token_pos: &TokenPos = match self.curr_token {
            None => return Err(ParseError::UnexepectedEoq),
            Some(ref token_pos) => token_pos,
        };

        if expected_tokens.contains(&(token_pos.token)) {
            Ok(token_pos.token.clone())
        } else {
            Err(ParseError::UnexpectedToken(token_pos.clone()))
        }
    }

    pub fn check_next_token(&self, expected_tokens: &[Token]) -> bool {
        match self.next_token {
            None => false,
            Some(ref token) => expected_tokens.contains(&token.token),
        }
    }

    pub fn validate_keyword(&self, expected_keywords: &[Keyword]) -> Result<Keyword, ParseError> {
        let token_pos: &TokenPos = match self.curr_token {
            None => return Err(ParseError::UnexepectedEoq),
            Some(ref ts) => ts,
        };

        let word: &str = match token_pos.token {
            Token::Word(ref s) => s,
            _ => return Err(ParseError::UnexpectedToken(token_pos.clone())),
        };

        let found_keyword: Keyword = match keyword_from_str(&word) {
            None => return Err(ParseError::UndefinedKeyword(token_pos.clone())),
            Some(keyword) => keyword,
        };

        if expected_keywords.contains(&found_keyword) {
            Ok(found_keyword)
        } else {
            Err(ParseError::UnexpectedKeyword(token_pos.clone()))
        }
    }

    pub fn check_next_keyword(&self, expected_keywords: &[Keyword]) -> bool {
        let token_pos: &TokenPos = match self.next_token {
            None => return false,
            Some(ref ts) => ts,
        };

        let keyword_opt: &str = match token_pos.token {
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
            }
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
            Keyword::Select => {
                let stmt: Statement = Statement::DML(DML::Select(try!(self.parse_select_stmt())));
                Ok(try!(self.build_ast(stmt)))
            }
            /*
            Keyword::Update => {
                let stmt: Statement = Statement::DML(DML::Update(try!(self.parse_update_stmt())));
                Ok(try!(self.build_ast(stmt)))
            },
            */
            Keyword::Insert => {
                let stmt: Statement = Statement::DML(DML::Insert(try!(self.parse_insert_stmt())));
                Ok(try!(self.build_ast(stmt)))
            }
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

    pub fn parse_create_columns(&mut self) -> Result<Vec<ColumnDef>, ParseError> {
        try!(self.bump());
        let mut columns: Vec<ColumnDef> = Vec::new();

        while !self.validate_token(&[Token::ClPar]).is_ok() {
            columns.push(try!(self.validate_column_def()));
            try!(self.bump());
            match try!(self.validate_token(&[Token::Comma, Token::ClPar])) {
                Token::Comma => try!(self.bump()),
                _ => (),
            }
        }
        Ok(columns)
    }

    pub fn parse_insert_stmt(&mut self) -> Result<InsertStmt, ParseError> {
        try!(self.bump());
        try!(self.validate_keyword(&[Keyword::Into]));

        try!(self.bump());
        let stmt: InsertStmt = InsertStmt {
            table_name: try!(self.validate_word(false)),
            column_names: try!(self.parse_insert_columns()),
            values: try!(self.parse_insert_values()),
        };

        if stmt.column_names.len() != stmt.values.len() {
            return Err(ParseError::MissmatchColumnNumber);
        }
        Ok(stmt)
    }

    pub fn parse_insert_columns(&mut self) -> Result<Vec<String>, ParseError> {
        try!(self.bump());
        try!(self.validate_token(&[Token::OpPar]));

        let mut column_names: Vec<String> = Vec::new();
        try!(self.bump());
        while !self.validate_token(&[Token::ClPar]).is_ok() {
            column_names.push(try!(self.validate_word(true)));
            try!(self.bump());
            match try!(self.validate_token(&[Token::Comma, Token::ClPar])) {
                Token::Comma => try!(self.bump()),
                _ => (),
            }
        }

        match column_names.len() {
            0 => Err(ParseError::MissmatchColumnNumber),
            _ => Ok(column_names),
        }
    }

    pub fn parse_insert_values(&mut self) -> Result<Vec<Literal>, ParseError> {
        try!(self.bump());
        try!(self.validate_keyword(&[Keyword::Values]));

        try!(self.bump());
        try!(self.validate_token(&[Token::OpPar]));

        let mut values: Vec<Literal> = Vec::new();
        try!(self.bump());
        while !self.validate_token(&[Token::ClPar]).is_ok() {
            values.push(try!(self.validate_literal()));
            try!(self.bump());
            match try!(self.validate_token(&[Token::Comma, Token::ClPar])) {
                Token::Comma => try!(self.bump()),
                _ => (),
            }
        }

        match values.len() {
            0 => Err(ParseError::MissmatchColumnNumber),
            _ => Ok(values),
        }
    }

    // TODO: impl alias
    pub fn parse_select_stmt(&mut self) -> Result<SelectStmt, ParseError> {
        // SELECT xx, yy
        try!(self.bump());
        let targets: Vec<Projectable> = try!(self.parse_projectable());

        // FROM xx, yy
        try!(self.bump());
        let sources: DataSource = try!(self.parse_from());

        // WHERE xx and yy
        let mut conditions: Option<Conditions> = None;
        if self.validate_keyword(&[Keyword::Where]).is_ok() {
            conditions = Some(try!(self.parse_conditions()));
        }

        // GROUP BY xx, yy
        let mut group_by: Option<Vec<Target>> = None;
        if self.validate_keyword(&[Keyword::Group]).is_ok() {
            group_by = Some(try!(self.parse_groupby()));
        }

        // ORDER BY xx ASC, yy DESC
        let mut order_by: Option<OrderBy> = None;
        if self.validate_keyword(&[Keyword::Order]).is_ok() {
            try!(self.bump());
            try!(self.validate_keyword(&[Keyword::By]));
            try!(self.bump());
            order_by = Some(try!(self.parse_orderby()));
        }

        // LIMIT xx
        let mut limit: Option<Limit> = None;
        if self.validate_keyword(&[Keyword::Limit]).is_ok() {
            try!(self.bump());
            limit = Some(try!(self.parse_limit()));
        }

        Ok(SelectStmt {
            targets: targets,
            source: sources,
            condition: conditions,
            group_by: group_by,
            order_by: order_by,
            limit: limit,
        })
    }

    pub fn parse_projectable(&mut self) -> Result<Vec<Projectable>, ParseError> {
        let mut targets: Vec<Projectable> = Vec::new();
        while !self.validate_keyword(&[Keyword::From]).is_ok() {
            match self.validate_token(&[Token::Comma]) {
                Ok(_t) => try!(self.bump()),
                Err(_e) => (),
            };

            match self.validate_token(&[Token::Star]) {
                Ok(_t) => {
                    targets.push(Projectable::All);
                    try!(self.bump());
                    continue;
                }
                Err(_e) => (),
            };

            match self.validate_keyword(&[
                Keyword::Count,
                Keyword::Sum,
                Keyword::Avg,
                Keyword::Max,
                Keyword::Min,
            ]) {
                Ok(_k) => match self.parse_aggregate() {
                    Ok(agg) => {
                        targets.push(Projectable::Aggregate(agg));
                        continue;
                    }
                    Err(_e) => (),
                },
                Err(_e) => (),
            };

            match self.validate_literal() {
                Ok(l) => {
                    targets.push(Projectable::Lit(l));
                    try!(self.bump());
                    continue;
                }
                Err(_e) => (),
            };

            match self.parse_target() {
                Ok(target) => targets.push(Projectable::Target(target)),
                Err(_e) => (),
            };
        }
        Ok(targets)
    }

    pub fn parse_aggregate(&mut self) -> Result<Aggregate, ParseError> {
        match try!(self.validate_keyword(&[
            Keyword::Count,
            Keyword::Sum,
            Keyword::Avg,
            Keyword::Max,
            Keyword::Min,
        ])) {
            Keyword::Count => {
                try!(self.bump());

                try!(self.validate_token(&[Token::OpPar]));
                try!(self.bump());

                let target: Aggregatable = try!(self.parse_aggregatable());

                try!(self.validate_token(&[Token::ClPar]));
                try!(self.bump());

                Ok(Aggregate::Count(target))
            }

            Keyword::Sum => {
                try!(self.bump());

                try!(self.validate_token(&[Token::OpPar]));
                try!(self.bump());

                let target: Aggregatable = try!(self.parse_aggregatable());

                try!(self.validate_token(&[Token::ClPar]));
                try!(self.bump());

                Ok(Aggregate::Sum(target))
            }

            Keyword::Avg => {
                try!(self.bump());

                try!(self.validate_token(&[Token::OpPar]));
                try!(self.bump());

                let target: Aggregatable = try!(self.parse_aggregatable());

                try!(self.validate_token(&[Token::ClPar]));
                try!(self.bump());

                Ok(Aggregate::Average(target))
            }

            Keyword::Max => {
                try!(self.bump());

                try!(self.validate_token(&[Token::OpPar]));
                try!(self.bump());

                let target: Aggregatable = try!(self.parse_aggregatable());

                try!(self.validate_token(&[Token::ClPar]));
                try!(self.bump());

                Ok(Aggregate::Max(target))
            }

            Keyword::Min => {
                try!(self.bump());

                try!(self.validate_token(&[Token::OpPar]));
                try!(self.bump());

                let target: Aggregatable = try!(self.parse_aggregatable());

                try!(self.validate_token(&[Token::ClPar]));
                try!(self.bump());

                Ok(Aggregate::Min(target))
            }
            _ => return Err(ParseError::SystemError),
        }
    }

    pub fn parse_aggregatable(&mut self) -> Result<Aggregatable, ParseError> {
        match self.validate_token(&[Token::Star]) {
            Ok(_t) => {
                try!(self.bump());
                Ok(Aggregatable::All)
            }
            Err(_e) => {
                let target: Target = try!(self.parse_target());
                Ok(Aggregatable::Target(target))
            }
        }
    }

    pub fn parse_target(&mut self) -> Result<Target, ParseError> {
        match try!(self.validate_word(false)) {
            _ => {
                let mut table_name: Option<String> = None;
                if self.check_next_token(&[Token::Dot]) {
                    table_name = Some(try!(self.validate_word(false)));
                    try!(self.double_bump());
                };

                let column_name: String = try!(self.validate_word(true));
                try!(self.bump());
                Ok(Target {
                    table_name: table_name,
                    name: column_name,
                })
            }
        }
    }

    pub fn parse_from(&mut self) -> Result<DataSource, ParseError> {
        let source: DataSource = DataSource::Leaf(try!(self.parse_data_source()));

        while self.validate_keyword(&[Keyword::Join]).is_ok()
            || self.validate_token(&[Token::Comma]).is_ok()
        {
            if self.validate_keyword(&[Keyword::Join]).is_ok() {
                try!(self.bump());
                return Ok(DataSource::Join(
                    Box::new(source),
                    Box::new(try!(self.parse_from())),
                    Some(try!(self.parse_conditions())),
                ));
            } else {
                try!(self.bump());
                return Ok(DataSource::Join(
                    Box::new(source),
                    Box::new(try!(self.parse_from())),
                    None,
                ));
            }
        }

        Ok(source)
    }

    pub fn parse_data_source(&mut self) -> Result<Source, ParseError> {
        let table = Source::Table(Table {
            name: try!(self.validate_word(true)),
        });
        try!(self.bump());
        Ok(table)
    }

    pub fn parse_conditions(&mut self) -> Result<Conditions, ParseError> {
        let mut cond;
        if self.check_next_token(&[Token::OpPar]) {
            try!(self.bump());
            cond = try!(self.parse_conditions());
            try!(self.validate_token(&[Token::ClPar]));
            if self.check_next_keyword(&[Keyword::And, Keyword::Or]) {
                try!(self.bump());
                match try!(self.validate_keyword(&[Keyword::And, Keyword::Or])) {
                    Keyword::And => {
                        cond = Conditions::And(
                            Box::new(cond),
                            Box::new(try!(self.parse_conditions())),
                        );
                    }
                    Keyword::Or => {
                        cond =
                            Conditions::Or(Box::new(cond), Box::new(try!(self.parse_conditions())));
                    }
                    _ => {}
                };
            };
        } else {
            cond = Conditions::Leaf(try!(self.parse_condition()));
            while self.validate_keyword(&[Keyword::And, Keyword::Or]).is_ok() {
                match try!(self.validate_keyword(&[Keyword::And, Keyword::Or])) {
                    Keyword::And => {
                        if self.check_next_token(&[Token::OpPar]) {
                            cond = Conditions::And(
                                Box::new(cond),
                                Box::new(try!(self.parse_conditions())),
                            );
                        } else {
                            cond = Conditions::And(
                                Box::new(cond),
                                Box::new(Conditions::Leaf(try!(self.parse_condition()))),
                            );
                            try!(self.bump());
                        };
                    }
                    Keyword::Or => {
                        cond =
                            Conditions::Or(Box::new(cond), Box::new(try!(self.parse_conditions())));
                    }
                    _ => return Err(ParseError::UndefinedStatementError),
                };
            }
        }
        try!(self.bump());
        Ok(cond)
    }

    pub fn parse_condition(&mut self) -> Result<Condition, ParseError> {
        try!(self.bump());
        let mut left_table_name: Option<String> = None;
        if self.check_next_token(&[Token::Dot]) {
            left_table_name = Some(try!(self.validate_word(false)));
            try!(self.double_bump());
        };

        let left_column_name: String = try!(self.validate_word(true));
        let left_side: Target = Target {
            table_name: left_table_name,
            name: left_column_name,
        };

        try!(self.bump());
        let op: Operator = match try!(self.validate_token(condition_tokens())) {
            Token::Equ => Operator::Equ,
            Token::NEqu => Operator::NEqu,
            Token::GT => Operator::GT,
            Token::LT => Operator::LT,
            Token::GE => Operator::GE,
            Token::LE => Operator::LE,
            _ => match self.curr_token {
                None => return Err(ParseError::UnexepectedEoq),
                Some(ref ts) => return Err(ParseError::UnexpectedToken(ts.clone())),
            },
        };

        try!(self.bump());
        let right_side: Comparable = match self.validate_word(false) {
            Ok(_right) => {
                let mut right_table_name: Option<String> = None;
                if self.check_next_token(&[Token::Dot]) {
                    right_table_name = Some(try!(self.validate_word(false)));
                    try!(self.double_bump());
                };

                let right_column_name: String = try!(self.validate_word(true));
                Comparable::Target(Target {
                    table_name: right_table_name,
                    name: right_column_name,
                })
            }
            _ => Comparable::Lit(try!(self.validate_literal())),
        };

        Ok(Condition {
            left: left_side,
            op: op,
            right: right_side,
        })
    }

    pub fn parse_groupby(&mut self) -> Result<Vec<Target>, ParseError> {
        let mut group_by: Vec<Target> = Vec::new();
        try!(self.bump());
        try!(self.validate_keyword(&[Keyword::By]));
        try!(self.bump());

        loop {
            group_by.push(try!(self.parse_target()));
            if self.validate_token(&[Token::Comma]).is_ok() {
                try!(self.bump());
                continue;
            } else {
                break;
            }
        }
        //println!("{:?}", group_by);
        Ok(group_by)
    }

    pub fn parse_orderby(&self) -> Result<OrderBy, ParseError> {
        Err(ParseError::UndefinedStatementError)
    }

    pub fn parse_limit(&self) -> Result<Limit, ParseError> {
        Err(ParseError::UndefinedStatementError)
    }
}

fn condition_tokens() -> &'static [Token] {
    &[
        Token::Equ,
        Token::GT,
        Token::LT,
        Token::GE,
        Token::LE,
        Token::NEqu,
    ]
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
        "count" => Some(Keyword::Count),
        "sum" => Some(Keyword::Sum),
        "max" => Some(Keyword::Max),
        "min" => Some(Keyword::Min),
        "avg" => Some(Keyword::Avg),
        "from" => Some(Keyword::From),
        "join" => Some(Keyword::Join),
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
        "on" => Some(Keyword::On),
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
    SystemError,
    ReservedKeyword(TokenPos),
    InvalidEoq,
    UndefinedStatementError,
    UndefinedKeyword(TokenPos),
    UndefinedDatatype(TokenPos),
    UndefinedNumber(TokenPos),
    UnexepectedEoq,
    UnexpectedKeyword(TokenPos),
    UnexpectedDatatype(TokenPos),
    UnexpectedToken(TokenPos),
    MissmatchColumnNumber,
}

impl From<LexError> for ParseError {
    fn from(err: LexError) -> ParseError {
        ParseError::LexError(err)
    }
}

#[cfg(test)]
mod tests {}
