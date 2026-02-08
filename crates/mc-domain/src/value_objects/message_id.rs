use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Domain-level unique message identifier (UUID v7, time-ordered).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MessageId(Uuid);

impl MessageId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for MessageId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_generates_unique_ids() {
        let a = MessageId::new();
        let b = MessageId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn from_uuid_roundtrip() {
        let uuid = Uuid::now_v7();
        let id = MessageId::from_uuid(uuid);
        assert_eq!(*id.as_uuid(), uuid);
    }

    #[test]
    fn display_trait() {
        let id = MessageId::new();
        let display = format!("{id}");
        assert!(!display.is_empty());
        assert_eq!(display.len(), 36);
    }
}
