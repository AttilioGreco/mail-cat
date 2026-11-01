//! TOON (Token-Oriented Object Notation), written by hand: the subset the
//! `mc labels` commands print. Tables are `name[N]{a,b}:` then one
//! comma-separated row per line, indented two spaces; objects are `key: value`
//! lines. Cheaper for an assistant to read than JSON, as field names appear once.
//!
//! A string is quoted (JSON escaping inside) when it is empty, has spaces at
//! either end, contains a comma, colon, quote, backslash, bracket, brace or
//! control character, starts with `-`, or would read back as a number,
//! boolean or null. A list inside a cell is its items joined with `|` (an item
//! that holds a `|` is ambiguous).

use std::fmt::Display;

/// One cell or value of a string.
pub fn s(text: &str) -> String {
    let plain = !text.is_empty()
        && text == text.trim()
        && !text.starts_with('-')
        && !text
            .chars()
            .any(|c| c.is_control() || ",:\"\\[]{}".contains(c))
        && !matches!(text, "true" | "false" | "null")
        && text.parse::<f64>().is_err();
    if plain {
        return text.to_owned();
    }
    serde_json::to_string(text).expect("a string always serializes")
}

/// A number or boolean.
pub fn n(value: impl Display) -> String {
    value.to_string()
}

/// A list inside a cell.
pub fn l<T: AsRef<str>>(items: impl IntoIterator<Item = T>) -> String {
    let items: Vec<T> = items.into_iter().collect();
    s(&items
        .iter()
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join("|"))
}

#[derive(Default)]
pub struct Toon(String);

impl Toon {
    /// `key: value`, `value` made by [`s`], [`n`] or [`l`].
    pub fn field(&mut self, key: &str, value: String) -> &mut Self {
        self.0.push_str(&format!("{key}: {value}\n"));
        self
    }

    /// `key[N]: a,b,c`
    pub fn list<T: AsRef<str>>(&mut self, key: &str, items: &[T]) -> &mut Self {
        let cells: Vec<String> = items.iter().map(|i| s(i.as_ref())).collect();
        self.0
            .push_str(&format!("{key}[{}]: {}\n", cells.len(), cells.join(",")));
        self
    }

    /// `name[N]{a,b}:` and a row per line; cells made by [`s`], [`n`] or [`l`].
    pub fn table(&mut self, name: &str, fields: &[&str], rows: &[Vec<String>]) -> &mut Self {
        self.0.push_str(&format!(
            "{name}[{}]{{{}}}:\n",
            rows.len(),
            fields.join(",")
        ));
        for row in rows {
            self.0.push_str(&format!("  {}\n", row.join(",")));
        }
        self
    }

    pub fn print(&self) {
        print!("{}", self.0);
    }

    #[cfg(test)]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests;
