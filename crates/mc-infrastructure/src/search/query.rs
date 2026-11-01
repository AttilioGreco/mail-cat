//! Parser for the Gmail-style search language (`docs/search.md`) into an AST.
//!
//! `OR` binds tighter than the implicit AND, as in Gmail: `a b OR c` is
//! `a AND (b OR c)`. A word with an unknown operator (`foo:bar`) or an invalid
//! value for a known one (`is:maybe`) is plain text, so pasted URLs and
//! subjects with colons still search; only malformed dates are errors.

use chrono::{DateTime, Months, NaiveDate, TimeDelta, Utc};

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    /// Free text over subject, sender, recipients and body. A quoted phrase
    /// is the same thing: words the tokenizer splits are matched adjacent.
    Text(String),
    /// Text restricted to one header-like field.
    Field(TextField, String),
    Label(String),
    In(String),
    Account(String),
    State(State),
    /// The message date is at or after the timestamp.
    Since(i64),
    /// The message date is before the timestamp.
    Until(i64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextField {
    From,
    To,
    Cc,
    Subject,
    Filename,
    List,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Unread,
    Read,
    Starred,
    Answered,
    Attachment,
}

impl Expr {
    /// The free-text terms outside any negation: what a snippet highlights.
    pub fn positive_text<'a>(&'a self, out: &mut Vec<&'a str>) {
        match self {
            Self::And(items) | Self::Or(items) => items.iter().for_each(|e| e.positive_text(out)),
            Self::Text(value) => out.push(value),
            _ => {}
        }
    }
}

pub fn parse(input: &str, now: DateTime<Utc>) -> Result<Expr, String> {
    let mut parser = Parser {
        tokens: tokenize(input),
        pos: 0,
        now,
    };
    let mut items = Vec::new();
    while parser.pos < parser.tokens.len() {
        // A stray ")" is dropped rather than failing a half-typed query.
        if parser.peek() == Some(&Token::Close) {
            parser.pos += 1;
            continue;
        }
        items.push(parser.or_group()?);
    }
    Ok(Expr::And(items))
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Open,
    Close,
    Or,
    Word(String),
    Phrase(String),
    /// `name:value`; the value may have been quoted or left empty for the
    /// next token.
    Operator {
        negated: bool,
        name: String,
        value: String,
    },
}

fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            c if c.is_whitespace() => {
                chars.next();
            }
            '(' | ')' => {
                chars.next();
                tokens.push(if c == '(' { Token::Open } else { Token::Close });
            }
            '"' => {
                chars.next();
                tokens.push(Token::Phrase(take_quoted(&mut chars)));
            }
            _ => {
                let mut word = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() || c == '(' || c == ')' {
                        break;
                    }
                    if c == '"' && word.ends_with(':') {
                        chars.next();
                        word.push_str(&take_quoted(&mut chars));
                        tokens.push(operator(word.clone(), true));
                        word.clear();
                        break;
                    }
                    word.push(c);
                    chars.next();
                }
                if !word.is_empty() {
                    tokens.push(operator(word, false));
                }
            }
        }
    }
    join_spaced_operators(tokens)
}

/// Reads up to the closing quote; an unterminated phrase runs to the end.
fn take_quoted(chars: &mut std::iter::Peekable<std::str::Chars>) -> String {
    let mut out = String::new();
    for c in chars.by_ref() {
        if c == '"' {
            break;
        }
        out.push(c);
    }
    out
}

fn operator(word: String, quoted: bool) -> Token {
    if !quoted && word == "OR" {
        return Token::Or;
    }
    let (negated, rest) = match word.strip_prefix('-') {
        Some(rest) if !rest.is_empty() => (true, rest),
        _ => (false, word.as_str()),
    };
    match rest.split_once(':') {
        Some((name, value)) if is_operator(name) => Token::Operator {
            negated,
            name: name.to_lowercase(),
            value: value.to_owned(),
        },
        _ => Token::Word(word),
    }
}

const OPERATORS: &[&str] = &[
    "from",
    "to",
    "cc",
    "subject",
    "sbj",
    "label",
    "in",
    "account",
    "is",
    "has",
    "filename",
    "after",
    "before",
    "newer_than",
    "older_than",
    "list",
];

fn is_operator(name: &str) -> bool {
    OPERATORS.contains(&name.to_lowercase().as_str())
}

/// `from: alice` (a space after the colon, the older syntax) reads as `from:alice`.
fn join_spaced_operators(tokens: Vec<Token>) -> Vec<Token> {
    let mut out: Vec<Token> = Vec::with_capacity(tokens.len());
    for token in tokens {
        match (out.last_mut(), token) {
            (Some(Token::Operator { value, .. }), Token::Word(next) | Token::Phrase(next))
                if value.is_empty() =>
            {
                *value = next;
            }
            (_, token) => out.push(token),
        }
    }
    out
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    now: DateTime<Utc>,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn or_group(&mut self) -> Result<Expr, String> {
        let mut alternatives = vec![self.unary()?];
        while self.peek() == Some(&Token::Or) {
            self.pos += 1;
            // A trailing OR ("a OR") has nothing to join: ignore it.
            if self.pos >= self.tokens.len() {
                break;
            }
            alternatives.push(self.unary()?);
        }
        Ok(match alternatives.len() {
            1 => alternatives.remove(0),
            _ => Expr::Or(alternatives),
        })
    }

    fn unary(&mut self) -> Result<Expr, String> {
        let token = self.tokens[self.pos].clone();
        self.pos += 1;
        Ok(match token {
            Token::Open => {
                let mut items = Vec::new();
                while !matches!(self.peek(), None | Some(Token::Close)) {
                    items.push(self.or_group()?);
                }
                self.pos += 1;
                Expr::And(items)
            }
            Token::Close => Expr::And(Vec::new()),
            Token::Or => text("OR"),
            Token::Phrase(value) => Expr::Text(value),
            Token::Word(word) => match word.strip_prefix('-') {
                // "-(a b)": the minus is its own word.
                Some("") if self.pos < self.tokens.len() => Expr::Not(Box::new(self.unary()?)),
                Some(rest) if !rest.is_empty() => Expr::Not(Box::new(text(rest))),
                _ => text(&word),
            },
            Token::Operator {
                negated,
                name,
                value,
            } => {
                let found = self.operator(&name, &value)?;
                if negated {
                    Expr::Not(Box::new(found))
                } else {
                    found
                }
            }
        })
    }

    fn operator(&self, name: &str, value: &str) -> Result<Expr, String> {
        let value = value.trim();
        let field = |f| Expr::Field(f, value.to_owned());
        Ok(match name {
            _ if value.is_empty() => text(&format!("{name}:")),
            "from" => field(TextField::From),
            "to" => field(TextField::To),
            "cc" => field(TextField::Cc),
            "subject" | "sbj" => field(TextField::Subject),
            "filename" => field(TextField::Filename),
            "list" => field(TextField::List),
            "label" => Expr::Label(value.to_owned()),
            "in" => Expr::In(value.to_owned()),
            "account" => Expr::Account(value.to_owned()),
            "is" => match value.to_lowercase().as_str() {
                "unread" => Expr::State(State::Unread),
                "read" => Expr::State(State::Read),
                "starred" => Expr::State(State::Starred),
                "answered" => Expr::State(State::Answered),
                _ => text(&format!("is:{value}")),
            },
            "has" if value.eq_ignore_ascii_case("attachment") => Expr::State(State::Attachment),
            "has" => text(&format!("has:{value}")),
            "after" => Expr::Since(day_start(name, value)?),
            "before" => Expr::Until(day_start(name, value)?),
            "newer_than" => Expr::Since(self.ago(name, value)?),
            "older_than" => Expr::Until(self.ago(name, value)?),
            _ => unreachable!("is_operator lists exactly these names"),
        })
    }

    /// `now` minus a `<n>d|m|y` span.
    fn ago(&self, name: &str, value: &str) -> Result<i64, String> {
        let bad = || format!("{name}:{value}: expected a number and d, m or y, like 7d");
        let split = value.len().saturating_sub(1);
        let (count, unit) = value.split_at_checked(split).ok_or_else(bad)?;
        let count: u32 = count.parse().map_err(|_| bad())?;
        let then = match unit.to_ascii_lowercase().as_str() {
            "d" => self.now.checked_sub_signed(TimeDelta::days(count.into())),
            "m" => self.now.checked_sub_months(Months::new(count)),
            "y" => count
                .checked_mul(12)
                .and_then(|m| self.now.checked_sub_months(Months::new(m))),
            _ => None,
        };
        then.map(|t| t.timestamp()).ok_or_else(bad)
    }
}

fn text(word: &str) -> Expr {
    Expr::Text(word.to_owned())
}

/// UTC midnight of a `YYYY-MM-DD` (or `YYYY/MM/DD`) day.
fn day_start(name: &str, value: &str) -> Result<i64, String> {
    NaiveDate::parse_from_str(&value.replace('/', "-"), "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .map(|d| d.and_utc().timestamp())
        .ok_or_else(|| format!("{name}:{value}: expected a date like 2026-01-31"))
}

#[cfg(test)]
mod tests;
