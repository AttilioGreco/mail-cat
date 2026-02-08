use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Unique attachment identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AttachmentId(Uuid);

impl AttachmentId {
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

impl Default for AttachmentId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for AttachmentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An email attachment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attachment {
    pub id: AttachmentId,
    pub filename: Option<String>,
    pub mime_type: String,
    pub size_bytes: u64,
    pub content_id: Option<String>,
    pub is_inline: bool,
}
