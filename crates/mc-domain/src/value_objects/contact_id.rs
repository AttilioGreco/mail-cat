use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Domain-level unique contact identifier (UUID v7, time-ordered).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContactId(Uuid);

impl ContactId {
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

impl Default for ContactId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ContactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for ContactId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_generates_unique_ids() {
        let a = ContactId::new();
        let b = ContactId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn display_and_roundtrip() {
        let uuid = Uuid::now_v7();
        let id = ContactId::from_uuid(uuid);
        assert_eq!(*id.as_uuid(), uuid);
        assert_eq!(format!("{id}").len(), 36);
    }
}
