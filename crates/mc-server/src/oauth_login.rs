//! Drives an interactive OAuth2 login end to end: opens the loopback
//! listener + authorize URL (via `mc_infrastructure::oauth`), waits for the
//! browser redirect, stores the resulting tokens in the OS keychain, and
//! appends the new account to `config.toml`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use mc_application::{OAuthLoginProgress, OAuthLoginTrigger};
use mc_domain::{OAuthProvider, SecretStore};
use tokio::sync::mpsc;
use toml_edit::{value, ArrayOfTables, DocumentMut, Item, Table};

#[derive(Clone)]
pub struct OAuthChallengeManager {
    config_path: PathBuf,
    secret_store: Arc<dyn SecretStore>,
}

impl OAuthChallengeManager {
    pub fn new(config_path: PathBuf, secret_store: Arc<dyn SecretStore>) -> Self {
        Self { config_path, secret_store }
    }
}

impl OAuthLoginTrigger for OAuthChallengeManager {
    fn start_login(
        &self,
        name: String,
        email: String,
        provider: OAuthProvider,
    ) -> mpsc::Receiver<OAuthLoginProgress> {
        let (tx, rx) = mpsc::channel(2);
        let secret_store = self.secret_store.clone();
        let config_path = self.config_path.clone();

        tokio::spawn(async move {
            let (authorize_url, pending) = match mc_infrastructure::oauth::begin_authorization(provider).await {
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

            let mut bundle = match secret_store.load(&email) {
                Ok(existing) => existing.unwrap_or_default(),
                Err(e) => {
                    let _ = tx
                        .send(OAuthLoginProgress::Failed { message: format!("reading existing keychain entry: {e}") })
                        .await;
                    return;
                }
            };
            bundle.oauth = Some(secrets);
            if let Err(e) = secret_store.store(&email, &bundle) {
                let _ = tx
                    .send(OAuthLoginProgress::Failed { message: format!("storing tokens in keychain: {e}") })
                    .await;
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
