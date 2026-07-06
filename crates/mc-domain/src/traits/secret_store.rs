//! Port: per-account secrets stored in the OS keychain.
//!
//! Secrets never live in the config file or the event store. They are kept in
//! the operating system's password manager (Secret Service / Keychain /
//! Credential Manager) and looked up by account email.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::DomainError;

/// All secrets for a single account, stored as one JSON blob per keychain item.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AccountSecrets {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub imap_password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub smtp_password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth: Option<OAuthSecrets>,
}

/// OAuth2 tokens. `expires_at` is duplicated in the domain event so the refresh
/// scheduler does not need to touch the keychain just to check expiry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OAuthSecrets {
    pub access_token: String,
    pub refresh_token: String,
    pub expires_at: DateTime<Utc>,
}

/// Persistence of per-account secrets. Keyed by account email.
///
/// Synchronous: the OS keychain is a blocking syscall and calls are rare
/// (startup, config change, credential update).
pub trait SecretStore: Send + Sync {
    /// Returns the stored secrets for `email`, or `None` if the account has no
    /// keychain entry yet.
    fn load(&self, email: &str) -> Result<Option<AccountSecrets>, DomainError>;

    /// Stores (replacing) the secrets for `email`.
    fn store(&self, email: &str, secrets: &AccountSecrets) -> Result<(), DomainError>;

    /// Removes the keychain entry for `email`. A missing entry is not an error.
    fn delete(&self, email: &str) -> Result<(), DomainError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_roundtrip_json_omitting_absent_fields() {
        let s = AccountSecrets {
            imap_password: Some("hunter2".into()),
            ..Default::default()
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(!json.contains("smtp_password"), "absent fields must be omitted: {json}");
        assert_eq!(serde_json::from_str::<AccountSecrets>(&json).unwrap(), s);
    }
}
