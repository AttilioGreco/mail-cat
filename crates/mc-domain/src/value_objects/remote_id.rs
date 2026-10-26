use serde::{Deserialize, Serialize};
use std::fmt;

/// A provider's identifier for a message inside one mailbox. Opaque: only
/// the provider that issued it interprets it (an IMAP UID in decimal, a JMAP
/// email id). Unique within a mailbox, not across mailboxes.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RemoteId(String);

impl RemoteId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RemoteId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for RemoteId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for RemoteId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

/// Numeric ids (IMAP UIDs) in decimal.
impl From<u32> for RemoteId {
    fn from(id: u32) -> Self {
        Self(id.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_ids_are_decimal() {
        assert_eq!(RemoteId::from(42).as_str(), "42");
        assert_eq!(RemoteId::from(42), RemoteId::new("42"));
    }
}
