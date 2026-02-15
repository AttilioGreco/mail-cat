use mc_domain::DomainError;

/// Application-layer error type.
///
/// Wraps domain errors and adds application-specific concerns
/// (authorization, validation of use-case preconditions, etc.).
#[derive(Debug, thiserror::Error)]
pub enum ApplicationError {
    /// A domain rule was violated.
    #[error(transparent)]
    Domain(#[from] DomainError),

    /// The requested resource was not found.
    #[error("{0} not found: {1}")]
    NotFound(&'static str, String),

    /// A precondition for the command/query was not met.
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),

    /// The operation is not authorized.
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// Internal application error (bug, unexpected state).
    #[error("internal error: {0}")]
    Internal(String),

    /// A sync operation failed.
    #[error("sync failed: {0}")]
    SyncFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domain_error_converts_to_application_error() {
        let domain_err = DomainError::Validation("bad input".into());
        let app_err: ApplicationError = domain_err.into();
        assert!(matches!(app_err, ApplicationError::Domain(_)));
        assert!(app_err.to_string().contains("bad input"));
    }

    #[test]
    fn not_found_error_display() {
        let err = ApplicationError::NotFound("Account", "abc-123".into());
        assert_eq!(err.to_string(), "Account not found: abc-123");
    }
}
