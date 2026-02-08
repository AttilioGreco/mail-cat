use thiserror::Error;

/// Domain-level errors.
///
/// These represent business rule violations and domain constraints.
/// Infrastructure errors (I/O, network) are mapped to these at the boundary.
#[derive(Debug, Error)]
pub enum DomainError {
    // ── Validation Errors ───────────────────────────────────────────

    #[error("Invalid email address: {0}")]
    InvalidEmailAddress(String),

    #[error("Invalid mailbox path: {0}")]
    InvalidMailboxPath(String),

    #[error("Validation error: {0}")]
    Validation(String),

    // ── Not Found Errors ────────────────────────────────────────────

    #[error("Account not found: {0}")]
    AccountNotFound(String),

    #[error("Mailbox not found: {0}")]
    MailboxNotFound(String),

    #[error("Message not found: {0}")]
    MessageNotFound(String),

    // ── Provider Errors ─────────────────────────────────────────────

    #[error("Provider error: {0}")]
    Provider(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Connection timeout: {0}")]
    ConnectionTimeout(String),

    #[error("Operation not supported by provider: {0}")]
    NotSupported(String),

    // ── Event Store Errors ──────────────────────────────────────────

    #[error("Concurrency conflict: expected sequence {expected}, found {actual}")]
    ConcurrencyConflict { expected: u64, actual: u64 },

    #[error("Event store error: {0}")]
    EventStore(String),

    // ── Repository Errors ───────────────────────────────────────────

    #[error("Repository error: {0}")]
    Repository(String),

    // ── Search Errors ───────────────────────────────────────────────

    #[error("Search index error: {0}")]
    SearchIndex(String),

    #[error("Search query parse error: {0}")]
    SearchQuery(String),

    // ── Serialization ───────────────────────────────────────────────

    #[error("Serialization error: {0}")]
    Serialization(String),
}

impl From<serde_json::Error> for DomainError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialization(e.to_string())
    }
}
