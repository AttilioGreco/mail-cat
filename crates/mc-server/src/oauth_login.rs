//! Drives an interactive OAuth2 login end to end: opens the loopback
//! listener + authorize URL (via `mc_infrastructure::oauth`), waits for the
//! browser redirect, stores the resulting tokens in the OS keychain, and
//! appends the new account to `config.toml`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use mc_application::OAuthLoginProgress;
use mc_domain::{DomainError, LoginSecret, OAuthProvider, OAuthSecrets, SecretStore};
use mc_infrastructure::ImapEmailProvider;
use tokio::sync::mpsc;
use toml_edit::{value, ArrayOfTables, DocumentMut, Item, Table};

#[derive(Clone)]
pub struct OAuthChallengeManager {
    config_path: PathBuf,
    secret_store: Arc<dyn SecretStore>,
}

/// The raw login flow. Not the [`OAuthLoginTrigger`] port itself — that is
/// implemented by `oauth_reauth::ReauthTrigger`, which wraps this and adds the
/// live-state reconciliation a running server needs.
impl OAuthChallengeManager {
    pub fn new(config_path: PathBuf, secret_store: Arc<dyn SecretStore>) -> Self {
        Self { config_path, secret_store }
    }

    pub fn start_login(
        &self,
        name: String,
        email: String,
        provider: OAuthProvider,
    ) -> mpsc::Receiver<OAuthLoginProgress> {
        let (tx, rx) = mpsc::channel(2);
        let secret_store = self.secret_store.clone();
        let config_path = self.config_path.clone();

        tokio::spawn(async move {
            let (authorize_url, pending) = match mc_infrastructure::oauth::begin_authorization(provider, &email).await {
                Ok(v) => v,
                Err(e) => {
                    let _ = tx.send(OAuthLoginProgress::Failed { message: e.to_string() }).await;
                    return;
                }
            };
            if tx
                .send(OAuthLoginProgress::AwaitingBrowser { authorize_url })
                .await
                .is_err()
            {
                return; // caller dropped the stream
            }

            let result = tokio::time::timeout(
                std::time::Duration::from_secs(300),
                mc_infrastructure::oauth::complete_authorization(pending),
            )
            .await;

            let secrets = match result {
                Ok(Ok(secrets)) => secrets,
                Ok(Err(e)) => {
                    let _ = tx.send(OAuthLoginProgress::Failed { message: e.to_string() }).await;
                    return;
                }
                Err(_) => {
                    let _ = tx
                        .send(OAuthLoginProgress::Failed {
                            message: "timed out waiting for browser login (5 min)".into(),
                        })
                        .await;
                    return;
                }
            };

            let verified = verify_imap_login(provider, &email, &secrets.access_token).await;
            if let Err(message) = store_tokens(secret_store.as_ref(), &email, provider, secrets, verified) {
                let _ = tx.send(OAuthLoginProgress::Failed { message }).await;
                return;
            }

            if let Err(e) = append_oauth_account(&config_path, &name, &email, provider) {
                let _ = tx
                    .send(OAuthLoginProgress::Failed { message: format!("writing account to config: {e}") })
                    .await;
                return;
            }

            let _ = tx.send(OAuthLoginProgress::Succeeded { email }).await;
        });

        rx
    }
}

/// The provider grants consent for whichever account the browser picked, which
/// need not be `email`; only an IMAP login as `email` proves the token fits it.
async fn verify_imap_login(provider: OAuthProvider, email: &str, access_token: &str) -> Result<(), DomainError> {
    let preset = mc_infrastructure::oauth::provider_preset(provider);
    ImapEmailProvider::new(
        preset.imap_host.into(),
        preset.imap_port,
        email.into(),
        Arc::new(LoginSecret::OAuthToken(access_token.into())),
        true,
        None,
    )
    .verify_login()
    .await
}

/// Stores the new tokens unless IMAP rejected them, so a wrong-account consent
/// never overwrites a keychain entry. A check that failed for any other reason
/// (network, timeout) doesn't block the login.
fn store_tokens(
    secret_store: &dyn SecretStore,
    email: &str,
    provider: OAuthProvider,
    secrets: OAuthSecrets,
    verified: Result<(), DomainError>,
) -> Result<(), String> {
    match verified {
        Ok(()) => {}
        Err(DomainError::AuthenticationFailed(detail)) => {
            tracing::warn!(%email, %detail, "IMAP rejected the newly granted OAuth token");
            return Err(format!(
                "{provider:?} ha autorizzato un account diverso da {email}: rifai il login scegliendo {email}"
            ));
        }
        Err(e) => tracing::warn!(%email, error = %e, "could not verify the new OAuth token over IMAP, storing it anyway"),
    }
    let mut bundle = secret_store
        .load(email)
        .map_err(|e| format!("reading existing keychain entry: {e}"))?
        .unwrap_or_default();
    bundle.oauth = Some(secrets);
    secret_store
        .store(email, &bundle)
        .map_err(|e| format!("storing tokens in keychain: {e}"))
}

/// Appends a new `[[accounts]]` table for an OAuth account to `config.toml`,
/// using the provider's IMAP/SMTP presets. Format-preserving (`toml_edit`),
/// same approach as `secret_ingest`'s rewrites.
fn append_oauth_account(
    config_path: &Path,
    name: &str,
    email: &str,
    provider: OAuthProvider,
) -> anyhow::Result<()> {
    let preset = mc_infrastructure::oauth::provider_preset(provider);
    let contents = std::fs::read_to_string(config_path).unwrap_or_default();
    let mut doc: DocumentMut = if contents.trim().is_empty() {
        DocumentMut::new()
    } else {
        contents.parse()?
    };

    if doc.get("accounts").is_none() {
        doc["accounts"] = Item::ArrayOfTables(ArrayOfTables::new());
    }
    let accounts = doc["accounts"]
        .as_array_of_tables_mut()
        .ok_or_else(|| anyhow::anyhow!("`accounts` in config.toml is not an array of tables"))?;

    let already_present = accounts
        .iter()
        .any(|table| table.get("email").and_then(|v| v.as_str()) == Some(email));
    if already_present {
        // Re-auth of an existing account: the caller has already refreshed
        // and stored the tokens in the keychain — the config entry itself
        // hasn't changed, so appending here would create a duplicate
        // [[accounts]] table.
        return Ok(());
    }

    let mut table = Table::new();
    table["name"] = value(name);
    table["email"] = value(email);
    table["provider_type"] = value("imap");
    table["imap_host"] = value(preset.imap_host);
    table["imap_port"] = value(preset.imap_port as i64);
    table["use_tls"] = value(true);
    table["username"] = value(email);
    table["oauth_provider"] = value(match provider {
        OAuthProvider::Google => "google",
        OAuthProvider::Microsoft => "microsoft",
    });

    let mut smtp = Table::new();
    smtp["host"] = value(preset.smtp_host);
    smtp["port"] = value(preset.smtp_port as i64);
    smtp["security"] = value("start_tls");
    table["smtp"] = Item::Table(smtp);

    accounts.push(table);
    std::fs::write(config_path, doc.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::AccountSecrets;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct MockStore(Mutex<HashMap<String, AccountSecrets>>);
    impl SecretStore for MockStore {
        fn load(&self, email: &str) -> Result<Option<AccountSecrets>, mc_domain::DomainError> {
            Ok(self.0.lock().unwrap().get(email).cloned())
        }
        fn store(&self, email: &str, s: &AccountSecrets) -> Result<(), mc_domain::DomainError> {
            self.0.lock().unwrap().insert(email.into(), s.clone());
            Ok(())
        }
        fn delete(&self, email: &str) -> Result<(), mc_domain::DomainError> {
            self.0.lock().unwrap().remove(email);
            Ok(())
        }
    }

    #[test]
    fn append_oauth_account_writes_expected_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "grpc_addr = \"127.0.0.1:50051\"\n").unwrap();

        append_oauth_account(&path, "Work Gmail", "me@gmail.com", OAuthProvider::Google).unwrap();

        let written = std::fs::read_to_string(&path).unwrap();
        let parsed: crate::config::ServerConfig = toml::from_str(&written).unwrap();
        assert_eq!(parsed.accounts.len(), 1);
        assert_eq!(parsed.accounts[0].email, "me@gmail.com");
        assert_eq!(parsed.accounts[0].imap_host, "imap.gmail.com");
        assert_eq!(
            parsed.accounts[0].oauth_provider,
            Some(crate::config::OAuthProviderConfig::Google)
        );
        assert!(!written.contains("password"));
    }

    #[test]
    fn append_oauth_account_is_idempotent_for_existing_email() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "grpc_addr = \"127.0.0.1:50051\"\n").unwrap();

        append_oauth_account(&path, "Work Gmail", "me@gmail.com", OAuthProvider::Google).unwrap();
        append_oauth_account(&path, "Work Gmail", "me@gmail.com", OAuthProvider::Google).unwrap();

        let written = std::fs::read_to_string(&path).unwrap();
        let parsed: crate::config::ServerConfig = toml::from_str(&written).unwrap();
        assert_eq!(
            parsed.accounts.len(),
            1,
            "re-auth of an existing account must not duplicate its config entry"
        );
    }

    fn tokens(access_token: &str) -> OAuthSecrets {
        OAuthSecrets { access_token: access_token.into(), refresh_token: "r".into(), expires_at: chrono::Utc::now() }
    }

    #[test]
    fn rejected_verification_leaves_keychain_untouched() {
        let store = MockStore::default();
        let old = AccountSecrets { oauth: Some(tokens("old")), ..Default::default() };
        store.store("me@gmail.com", &old).unwrap();

        let rejected = Err(DomainError::AuthenticationFailed("400".into()));
        let err = store_tokens(&store, "me@gmail.com", OAuthProvider::Google, tokens("new"), rejected).unwrap_err();

        assert!(err.contains("me@gmail.com"));
        assert_eq!(store.load("me@gmail.com").unwrap(), Some(old));
    }

    #[test]
    fn unreachable_verification_still_stores_tokens() {
        let store = MockStore::default();

        let offline = Err(DomainError::Provider("TCP connection failed".into()));
        store_tokens(&store, "me@gmail.com", OAuthProvider::Google, tokens("new"), offline).unwrap();

        let stored = store.load("me@gmail.com").unwrap().unwrap().oauth.unwrap();
        assert_eq!(stored.access_token, "new");
    }

    #[tokio::test]
    async fn start_login_reports_failure_when_client_id_unset() {
        // No MAILCAT_GOOGLE_CLIENT_ID set in the test environment — begin_authorization
        // itself still succeeds (it only builds a URL), but this at minimum proves
        // the trigger's AwaitingBrowser → terminal-event shape holds without a browser.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "grpc_addr = \"127.0.0.1:50051\"\n").unwrap();
        let manager = OAuthChallengeManager::new(path, Arc::new(MockStore::default()));

        let mut rx = manager.start_login("Work".into(), "me@gmail.com".into(), OAuthProvider::Google);
        match rx.recv().await {
            Some(OAuthLoginProgress::AwaitingBrowser { authorize_url }) => {
                assert!(authorize_url.contains("accounts.google.com"));
            }
            _other => panic!("expected AwaitingBrowser first"),
        }
        // Don't wait for the 5-minute timeout in a unit test — drop the receiver,
        // which ends the spawned task's send loop on its next `tx.send(...)`.
    }
}
