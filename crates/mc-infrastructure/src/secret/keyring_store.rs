//! [`SecretStore`] backed by the OS keychain via the `keyring` crate.
//!
//! `keyring` v4 selects the native store per platform automatically (macOS
//! Keychain, Windows Credential Manager, Linux/BSD Secret Service). No fallback:
//! if no store is available, `Entry::new` fails and the error surfaces at
//! startup — mail-cat requires a desktop session (see design spec).

use keyring::Entry;
use mc_domain::{AccountSecrets, DomainError, SecretStore};

/// Keychain service name; the account email is the per-entry key.
const SERVICE: &str = "mail-cat";

#[derive(Debug, Default, Clone, Copy)]
pub struct KeyringSecretStore;

impl KeyringSecretStore {
    pub fn new() -> Self {
        Self
    }

    fn entry(email: &str) -> Result<Entry, DomainError> {
        Entry::new(SERVICE, email).map_err(|e| DomainError::SecretStore(e.to_string()))
    }
}

impl SecretStore for KeyringSecretStore {
    fn load(&self, email: &str) -> Result<Option<AccountSecrets>, DomainError> {
        match Self::entry(email)?.get_password() {
            Ok(json) => serde_json::from_str(&json).map(Some).map_err(|e| {
                DomainError::SecretStore(format!("corrupt secret blob for {email}: {e}"))
            }),
            Err(keyring::Error::NoEntry) => Ok(None),
            Err(e) => Err(DomainError::SecretStore(e.to_string())),
        }
    }

    fn store(&self, email: &str, secrets: &AccountSecrets) -> Result<(), DomainError> {
        let json =
            serde_json::to_string(secrets).map_err(|e| DomainError::SecretStore(e.to_string()))?;
        Self::entry(email)?
            .set_password(&json)
            .map_err(|e| DomainError::SecretStore(e.to_string()))
    }

    fn delete(&self, email: &str) -> Result<(), DomainError> {
        match Self::entry(email)?.delete_credential() {
            Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
            Err(e) => Err(DomainError::SecretStore(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exercises the real OS keychain. Ignored by default (needs a desktop
    /// session with Secret Service / Keychain / Credential Manager).
    /// Run with: `cargo test -p mc-infrastructure --ignored keychain_roundtrip`
    #[test]
    #[ignore]
    fn keychain_roundtrip() {
        let store = KeyringSecretStore::new();
        let email = "verify-mailcat@example.invalid";
        let secrets = AccountSecrets {
            password: Some("s3cr3t".into()),
            ..Default::default()
        };
        store.delete(email).unwrap(); // clean slate
        assert_eq!(store.load(email).unwrap(), None);
        store.store(email, &secrets).unwrap();
        assert_eq!(store.load(email).unwrap(), Some(secrets));
        store.delete(email).unwrap();
        assert_eq!(store.load(email).unwrap(), None);
    }
}
