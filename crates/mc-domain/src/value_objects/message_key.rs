use serde::{Deserialize, Serialize};
use std::fmt;

/// A provider's identifier for a message across all the mailboxes it is in
/// (Gmail's X-GM-MSGID, a JMAP email id). Opaque, and absent where the
/// provider cannot tell that two copies are one message.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageKey(String);

impl MessageKey {
    pub fn new(key: impl Into<String>) -> Self {
        Self(key.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MessageKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
