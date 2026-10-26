use serde::{Deserialize, Serialize};
use std::fmt;

/// Where a provider left off in one collection: handed back to it on the next
/// sync to get only what changed. Opaque and provider-owned (an IMAP
/// validity and next-id pair, a JMAP state string); the application stores it
/// and never looks inside.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SyncCursor(String);

impl SyncCursor {
    pub fn new(cursor: impl Into<String>) -> Self {
        Self(cursor.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SyncCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
