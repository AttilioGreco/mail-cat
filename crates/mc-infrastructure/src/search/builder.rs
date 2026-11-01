//! Turns a parsed query into Tantivy queries, field by field, so operators
//! never pass through the query parser's own syntax.

use std::ops::Bound;

use tantivy::query::{
    AllQuery, BooleanQuery, EmptyQuery, Occur, PhraseQuery, Query, RangeQuery, TermQuery,
};
use tantivy::schema::{Field, IndexRecordOption};
use tantivy::{Index, Term};

use mc_domain::model::mailbox::MailboxType;
use mc_domain::traits::search_index::SearchDirectory;

use super::query::{Expr, State, TextField};
use super::schema::{IndexFields, label_key};

pub(super) struct QueryBuilder<'a> {
    pub index: &'a Index,
    pub fields: &'a IndexFields,
    pub directory: &'a SearchDirectory,
}

type Clause = (Occur, Box<dyn Query>);

impl QueryBuilder<'_> {
    pub fn build(&self, expr: &Expr) -> Box<dyn Query> {
        self.expr(expr).unwrap_or_else(|| Box::new(EmptyQuery))
    }

    /// `None` for a part that constrains nothing (empty parentheses, text that
    /// has no searchable characters).
    fn expr(&self, expr: &Expr) -> Option<Box<dyn Query>> {
        match expr {
            Expr::And(items) => self.all(items),
            Expr::Or(items) => {
                let clauses: Vec<Clause> = items
                    .iter()
                    .filter_map(|e| self.expr(e))
                    .map(|q| (Occur::Should, q))
                    .collect();
                (!clauses.is_empty()).then(|| Box::new(BooleanQuery::new(clauses)) as _)
            }
            Expr::Not(inner) => self.expr(inner).map(|q| {
                Box::new(BooleanQuery::new(vec![
                    (Occur::Must, Box::new(AllQuery) as _),
                    (Occur::MustNot, q),
                ])) as _
            }),
            Expr::Text(value) => self.text(value),
            Expr::Field(field, value) => self.text_field(*field, value),
            Expr::Label(label) => Some(self.term(self.fields.labels, &label_key(label))),
            Expr::In(place) => Some(self.any_of(self.fields.mailbox_id, self.mailbox_ids(place))),
            Expr::Account(who) => Some(self.any_of(self.fields.account_id, self.account_ids(who))),
            Expr::State(state) => Some(self.term(self.fields.state, state_term(*state))),
            Expr::Since(at) => Some(self.dates(Bound::Included(*at), Bound::Unbounded)),
            Expr::Until(at) => Some(self.dates(Bound::Unbounded, Bound::Excluded(*at))),
        }
    }

    /// Implicit AND. Negations become `MustNot` beside the positive parts, and
    /// a group of only negations starts from every document.
    fn all(&self, items: &[Expr]) -> Option<Box<dyn Query>> {
        let mut clauses: Vec<Clause> = Vec::new();
        for item in items {
            match item {
                Expr::Not(inner) => clauses.extend(self.expr(inner).map(|q| (Occur::MustNot, q))),
                _ => clauses.extend(self.expr(item).map(|q| (Occur::Must, q))),
            }
        }
        match clauses.as_slice() {
            [] => return None,
            [(Occur::Must, _)] => return clauses.pop().map(|(_, q)| q),
            _ => {}
        }
        if clauses.iter().all(|(occur, _)| *occur == Occur::MustNot) {
            clauses.push((Occur::Must, Box::new(AllQuery)));
        }
        Some(Box::new(BooleanQuery::new(clauses)))
    }

    /// Free text: any of the content fields.
    fn text(&self, value: &str) -> Option<Box<dyn Query>> {
        let f = self.fields;
        self.in_any(
            &[f.subject, f.from_name, f.from_email, f.to, f.cc, f.body],
            value,
        )
    }

    fn in_any(&self, fields: &[Field], value: &str) -> Option<Box<dyn Query>> {
        let clauses: Vec<Clause> = fields
            .iter()
            .filter_map(|field| self.in_field(*field, value))
            .map(|q| (Occur::Should, q))
            .collect();
        (!clauses.is_empty()).then(|| Box::new(BooleanQuery::new(clauses)) as _)
    }

    /// The value's tokens as one term or, split by the field's tokenizer
    /// ("facile.it" -> facile, it), as a phrase.
    fn in_field(&self, field: Field, value: &str) -> Option<Box<dyn Query>> {
        let mut analyzer = self.index.tokenizer_for_field(field).ok()?;
        let mut stream = analyzer.token_stream(value);
        let mut terms = Vec::new();
        while stream.advance() {
            terms.push(Term::from_field_text(field, &stream.token().text));
        }
        match terms.len() {
            0 => None,
            1 => Some(Box::new(TermQuery::new(
                terms.remove(0),
                IndexRecordOption::Basic,
            ))),
            _ => Some(Box::new(PhraseQuery::new(terms))),
        }
    }

    fn text_field(&self, field: TextField, value: &str) -> Option<Box<dyn Query>> {
        let f = self.fields;
        let fields = match field {
            TextField::From => &[f.from_email, f.from_name][..],
            TextField::To => &[f.to],
            TextField::Cc => &[f.cc],
            TextField::Subject => &[f.subject],
            TextField::Filename => &[f.attachment],
            TextField::List => &[f.list_id],
        };
        self.in_any(fields, value)
    }

    fn term(&self, field: Field, value: &str) -> Box<dyn Query> {
        Box::new(TermQuery::new(
            Term::from_field_text(field, value),
            IndexRecordOption::Basic,
        ))
    }

    fn any_of(&self, field: Field, ids: Vec<String>) -> Box<dyn Query> {
        if ids.is_empty() {
            return Box::new(EmptyQuery);
        }
        Box::new(BooleanQuery::new(
            ids.iter()
                .map(|id| (Occur::Should, self.term(field, id)))
                .collect(),
        ))
    }

    fn dates(&self, from: Bound<i64>, to: Bound<i64>) -> Box<dyn Query> {
        let term = |bound: Bound<i64>| match bound {
            Bound::Included(t) => Bound::Included(Term::from_field_i64(self.fields.date, t)),
            Bound::Excluded(t) => Bound::Excluded(Term::from_field_i64(self.fields.date, t)),
            Bound::Unbounded => Bound::Unbounded,
        };
        Box::new(RangeQuery::new(term(from), term(to)))
    }

    /// `in:inbox` and friends are mailbox roles in any account; anything else
    /// is a mailbox display path.
    fn mailbox_ids(&self, place: &str) -> Vec<String> {
        let wanted: &[MailboxType] = match place.to_lowercase().as_str() {
            "inbox" => &[MailboxType::Inbox],
            "sent" => &[MailboxType::Sent],
            "trash" => &[MailboxType::Trash],
            "spam" => &[MailboxType::Spam],
            "drafts" => &[MailboxType::Drafts],
            "archive" => &[MailboxType::Archive, MailboxType::All],
            _ => &[],
        };
        self.directory
            .mailboxes
            .iter()
            .filter(|m| {
                wanted.contains(&m.mailbox_type)
                    || (wanted.is_empty() && m.display_path.eq_ignore_ascii_case(place))
            })
            .map(|m| m.id.to_string())
            .collect()
    }

    fn account_ids(&self, who: &str) -> Vec<String> {
        self.directory
            .accounts
            .iter()
            .filter(|a| {
                a.name.eq_ignore_ascii_case(who)
                    || a.email.as_str().eq_ignore_ascii_case(who)
                    || a.id.to_string().eq_ignore_ascii_case(who)
            })
            .map(|a| a.id.to_string())
            .collect()
    }
}

fn state_term(state: State) -> &'static str {
    match state {
        State::Unread => "unread",
        State::Read => "read",
        State::Starred => "starred",
        State::Answered => "answered",
        State::Attachment => "attachment",
    }
}
