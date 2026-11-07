//! vCard and iCalendar payloads, as text.
//!
//! The raw payload is the source of truth, so edits are applied to the text
//! itself: a property we do not edit is carried over byte for byte, folding
//! included. `calcard` parses (and expands recurrences) but is never used to
//! write: it re-serializes, which normalizes and could drop what it does not
//! model.

mod icalendar;
mod vcard;

use std::ops::Range;

use chrono::{DateTime, Utc};

pub use icalendar::{EventEdit, EventWhen, ParsedEvent, edit_event, occurrences, parse_event};
pub use vcard::{ContactEdit, ParsedCard, edit_vcard, parse_vcard};

use crate::error::DomainError;

/// One unfolded content line, kept as it was written.
#[derive(Debug, Clone)]
struct Line {
    /// Upper-cased property name, group prefix (`item1.`) removed.
    name: String,
    /// Original physical lines, continuation folds included, joined by `\n`.
    text: String,
}

impl Line {
    fn parse(text: String) -> Self {
        let head = text.split([':', ';']).next().unwrap_or_default();
        let name = head.rsplit('.').next().unwrap_or(head).to_ascii_uppercase();
        Self { name, text }
    }

    fn unfolded(&self) -> String {
        self.text.replace("\n ", "").replace("\n\t", "")
    }

    /// Index of the colon that ends the name and parameters.
    fn colon(unfolded: &str) -> Option<usize> {
        let mut quoted = false;
        unfolded.char_indices().find_map(|(i, c)| match c {
            '"' => {
                quoted = !quoted;
                None
            }
            ':' if !quoted => Some(i),
            _ => None,
        })
    }

    /// Raw value, still escaped.
    fn value(&self) -> String {
        let text = self.unfolded();
        Self::colon(&text).map_or_else(String::new, |i| text[i + 1..].to_owned())
    }

    /// `;NAME=value...` between the name and the colon, kept for rewriting a
    /// property without losing its parameters (TZID, VALUE=DATE).
    fn params(&self) -> String {
        let text = self.unfolded();
        let end = Self::colon(&text).unwrap_or(text.len());
        text[..end]
            .find(';')
            .map_or_else(String::new, |i| text[i..end].to_owned())
    }
}

/// A whole payload as lines.
struct Doc(Vec<Line>);

impl Doc {
    fn parse(raw: &str) -> Self {
        let mut lines: Vec<Line> = Vec::new();
        for physical in raw.lines() {
            match lines.last_mut() {
                Some(last) if physical.starts_with([' ', '\t']) => {
                    last.text.push('\n');
                    last.text.push_str(physical);
                }
                _ if physical.is_empty() => {}
                _ => lines.push(Line::parse(physical.to_owned())),
            }
        }
        Self(lines)
    }

    fn render(&self) -> String {
        let mut out = String::new();
        for line in &self.0 {
            out.push_str(&line.text.replace('\n', "\r\n"));
            out.push_str("\r\n");
        }
        out
    }

    /// The lines between `BEGIN:component` and its `END`, for each such
    /// component.
    fn components(&self, component: &str) -> Vec<Range<usize>> {
        let mut found = Vec::new();
        let mut open = None;
        for (i, line) in self.0.iter().enumerate() {
            let named = line.value().eq_ignore_ascii_case(component);
            match (line.name.as_str(), open) {
                ("BEGIN", None) if named => open = Some(i + 1),
                ("END", Some(start)) if named => {
                    found.push(start..i);
                    open = None;
                }
                _ => {}
            }
        }
        found
    }

    /// Lines of `scope` that belong to it, not to a component nested in it
    /// (a VALARM has its own SUMMARY).
    fn own(&self, scope: &Range<usize>) -> Vec<usize> {
        let mut depth = 0;
        scope
            .clone()
            .filter(|&i| match self.0[i].name.as_str() {
                "BEGIN" => {
                    depth += 1;
                    false
                }
                "END" => {
                    depth -= 1;
                    false
                }
                _ => depth == 0,
            })
            .collect()
    }

    fn first(&self, scope: &Range<usize>, name: &str) -> Option<&Line> {
        self.own(scope)
            .into_iter()
            .map(|i| &self.0[i])
            .find(|l| l.name == name)
    }

    /// Replaces the `name` lines of `scope` with `build`'s, which is given the
    /// old ones so it can keep those still wanted. The new lines go where the
    /// first old one was, or at the end of the scope. Ranges taken before the
    /// call are stale after it.
    fn replace(
        &mut self,
        scope: &Range<usize>,
        name: &str,
        build: impl FnOnce(&[Line]) -> Vec<String>,
    ) {
        let at: Vec<usize> = self
            .own(scope)
            .into_iter()
            .filter(|&i| self.0[i].name == name)
            .collect();
        let old: Vec<Line> = at.iter().map(|&i| self.0[i].clone()).collect();
        let new = build(&old).into_iter().map(Line::parse);
        let insert_at = at.first().copied().unwrap_or(scope.end);
        for &i in at.iter().rev() {
            self.0.remove(i);
        }
        self.0.splice(insert_at..insert_at, new);
    }
}

/// `NAME[params]:value`, folded at 75 octets.
fn content_line(name: &str, params: &str, value: &str) -> String {
    let line = format!("{name}{params}:{value}");
    let mut out = String::with_capacity(line.len() + line.len() / 70);
    let mut width = 0;
    for c in line.chars() {
        // A continuation line starts with a space, which counts toward the limit.
        if width + c.len_utf8() > 75 {
            out.push_str("\n ");
            width = 1;
        }
        out.push(c);
        width += c.len_utf8();
    }
    out
}

fn escape(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace(';', "\\;")
        .replace(',', "\\,")
        .replace("\r\n", "\\n")
        .replace('\n', "\\n")
}

fn timestamp(at: DateTime<Utc>) -> String {
    at.format("%Y%m%dT%H%M%SZ").to_string()
}

fn invalid(what: &str) -> DomainError {
    DomainError::Validation(format!("invalid {what} payload"))
}

#[cfg(test)]
mod tests;
