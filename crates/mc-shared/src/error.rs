use thiserror::Error;

/// Shared error types used across multiple layers.
#[derive(Debug, Error)]
pub enum SharedError {
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<serde_json::Error> for SharedError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}
