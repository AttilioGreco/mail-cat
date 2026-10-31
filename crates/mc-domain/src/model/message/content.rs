//! What readers of a message's content need beyond the envelope: the mailing
//! list headers, and the body as plain text.

use std::borrow::Cow;

use serde::{Deserialize, Serialize};

use super::Message;

/// The headers that tell bulk mail from correspondence. Only their presence
/// and kind are kept: they are classifier features, not content to show.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListHeaders {
    pub unsubscribe: bool,
    /// `List-Id` value, whatever the list calls itself.
    pub list_id: Option<String>,
    /// `Precedence: bulk | list | junk`.
    pub bulk: bool,
    /// `Auto-Submitted` other than `no`.
    pub auto_submitted: bool,
}

impl ListHeaders {
    /// Take one header field into account; other fields are ignored.
    pub fn note(&mut self, name: &str, value: &str) {
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "list-unsubscribe" => self.unsubscribe = !value.is_empty(),
            "list-id" => self.list_id = Some(value.to_owned()).filter(|v| !v.is_empty()),
            "precedence" => {
                self.bulk = ["bulk", "list", "junk"]
                    .iter()
                    .any(|p| value.eq_ignore_ascii_case(p));
            }
            "auto-submitted" => {
                self.auto_submitted = !value.is_empty() && !value.eq_ignore_ascii_case("no");
            }
            _ => {}
        }
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

impl Message {
    /// The body as text: the text part, or the tag-stripped HTML part when
    /// there is none. Search indexing and classification read the same text.
    pub fn plain_body(&self) -> Option<Cow<'_, str>> {
        match (&self.body_text, &self.body_html) {
            (Some(text), _) => Some(Cow::Borrowed(text)),
            (None, Some(html)) => Some(Cow::Owned(strip_html(html))),
            (None, None) => None,
        }
    }
}

/// Tag removal; entities and scripts are not interpreted.
pub fn strip_html(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for ch in html.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    out
}
