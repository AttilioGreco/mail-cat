//! Writes account declarations into the server's `config.toml`.
//!
//! Boot reads `config.toml` to build providers (see `wiring`), so this is what
//! makes an account added at runtime survive a restart — and a removed one stay
//! removed. Passwords go straight to the OS keychain; the config file never sees
//! them (unlike the hand-edited path, which `secret_ingest` cleans up at boot).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use mc_application::{AccountProvisioner, ApplicationError, ProvisionParams};
use mc_domain::model::account::SmtpSecurity;
use mc_domain::{IncomingServer, SecretStore};
use toml_edit::{ArrayOfTables, DocumentMut, Item, Table, value};

pub struct ConfigAccountProvisioner {
    config_path: PathBuf,
    secrets: Arc<dyn SecretStore>,
}

impl ConfigAccountProvisioner {
    pub fn new(config_path: PathBuf, secrets: Arc<dyn SecretStore>) -> Self {
        Self {
            config_path,
            secrets,
        }
    }
}

#[async_trait]
impl AccountProvisioner for ConfigAccountProvisioner {
    async fn provision(&self, params: ProvisionParams) -> Result<(), ApplicationError> {
        if !matches!(params.incoming, IncomingServer::Imap { .. }) {
            return Err(ApplicationError::Internal("JMAP not supported yet".into()));
        }
        // Keychain first: a crash between the two steps leaves an unused secret
        // (harmless), where the reverse order would leave an account that cannot
        // authenticate.
        let mut secrets = self
            .secrets
            .load(&params.email)
            .map_err(|e| ApplicationError::Internal(format!("keychain read failed: {e}")))?
            .unwrap_or_default();
        secrets.password = Some(params.password.clone());
        // Same credential for submission unless the user says otherwise — the
        // overwhelmingly common IMAP/SMTP setup.
        secrets.smtp_password = Some(params.password.clone());
        self.secrets
            .store(&params.email, &secrets)
            .map_err(|e| ApplicationError::Internal(format!("keychain write failed: {e}")))?;

        append_account(&self.config_path, &params)
            .map_err(|e| ApplicationError::Internal(format!("config write failed: {e}")))
    }

    async fn deprovision(&self, email: &str) -> Result<(), ApplicationError> {
        remove_account_table(&self.config_path, email)
            .map_err(|e| ApplicationError::Internal(format!("config write failed: {e}")))?;

        self.secrets
            .delete(email)
            .map_err(|e| ApplicationError::Internal(format!("keychain delete failed: {e}")))
    }

    async fn set_password(&self, email: &str, password: &str) -> Result<(), ApplicationError> {
        let mut secrets = self
            .secrets
            .load(email)
            .map_err(|e| ApplicationError::Internal(format!("keychain read failed: {e}")))?
            .unwrap_or_default();
        // Mirrors provision: one credential for both IMAP and SMTP.
        secrets.password = Some(password.to_owned());
        secrets.smtp_password = Some(password.to_owned());
        self.secrets
            .store(email, &secrets)
            .map_err(|e| ApplicationError::Internal(format!("keychain write failed: {e}")))
    }
}

/// Append an `[[accounts]]` table for a password-authenticated IMAP account.
/// Fails for any other incoming server: config.toml only describes IMAP.
/// Format-preserving, and a no-op when the email is already declared.
fn append_account(config_path: &Path, params: &ProvisionParams) -> anyhow::Result<()> {
    let IncomingServer::Imap {
        host,
        port,
        use_tls,
    } = &params.incoming
    else {
        anyhow::bail!("JMAP not supported yet");
    };
    let mut doc = load_document(config_path)?;

    if doc.get("accounts").is_none() {
        doc["accounts"] = Item::ArrayOfTables(ArrayOfTables::new());
    }
    let accounts = doc["accounts"]
        .as_array_of_tables_mut()
        .ok_or_else(|| anyhow::anyhow!("`accounts` in config.toml is not an array of tables"))?;

    if accounts
        .iter()
        .any(|t| t.get("email").and_then(|v| v.as_str()) == Some(params.email.as_str()))
    {
        // Already declared — re-adding the same address would create a duplicate
        // account block, and boot would then fight over which one owns the email.
        return Ok(());
    }

    let mut table = Table::new();
    table["name"] = value(&params.name);
    table["email"] = value(&params.email);
    table["provider_type"] = value("imap");
    table["imap_host"] = value(host);
    table["imap_port"] = value(*port as i64);
    table["use_tls"] = value(*use_tls);
    table["username"] = value(&params.username);

    if let Some(config) = &params.smtp {
        let mut smtp = Table::new();
        smtp["host"] = value(&config.host);
        smtp["port"] = value(config.port as i64);
        smtp["security"] = value(match config.security {
            SmtpSecurity::Smtps => "smtps",
            SmtpSecurity::StartTls => "start_tls",
        });
        table["smtp"] = Item::Table(smtp);
    }

    accounts.push(table);
    std::fs::write(config_path, doc.to_string())?;
    Ok(())
}

/// Delete the `[[accounts]]` table whose `email` matches. Missing is not an error.
fn remove_account_table(config_path: &Path, email: &str) -> anyhow::Result<()> {
    let mut doc = load_document(config_path)?;

    let Some(accounts) = doc
        .get_mut("accounts")
        .and_then(|i| i.as_array_of_tables_mut())
    else {
        return Ok(());
    };

    let before = accounts.len();
    accounts.retain(|t| t.get("email").and_then(|v| v.as_str()) != Some(email));
    if accounts.len() == before {
        return Ok(());
    }

    std::fs::write(config_path, doc.to_string())?;
    Ok(())
}

fn load_document(config_path: &Path) -> anyhow::Result<DocumentMut> {
    let contents = std::fs::read_to_string(config_path).unwrap_or_default();
    Ok(if contents.trim().is_empty() {
        DocumentMut::new()
    } else {
        contents.parse()?
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mc_domain::model::account::SmtpConnectionConfig;

    fn params(email: &str) -> ProvisionParams {
        ProvisionParams {
            name: "Work".into(),
            email: email.into(),
            incoming: IncomingServer::Imap {
                host: "mail.example.com".into(),
                port: 993,
                use_tls: true,
            },
            username: email.into(),
            password: "hunter2".into(),
            smtp: Some(SmtpConnectionConfig {
                host: "mail.example.com".into(),
                port: 587,
                security: SmtpSecurity::StartTls,
                username: None,
                password: None,
            }),
        }
    }

    #[test]
    fn appends_account_without_writing_the_password() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, "grpc_addr = \"127.0.0.1:50051\"\n").unwrap();

        append_account(&path, &params("me@example.com")).unwrap();

        let written = std::fs::read_to_string(&path).unwrap();
        assert!(
            !written.contains("hunter2"),
            "password must stay in the keychain: {written}"
        );

        let parsed: crate::config::ServerConfig = toml::from_str(&written).unwrap();
        assert_eq!(parsed.accounts.len(), 1);
        assert_eq!(parsed.accounts[0].email, "me@example.com");
        assert_eq!(parsed.accounts[0].imap_port, 993);
        assert_eq!(parsed.accounts[0].smtp.as_ref().unwrap().port, 587);
    }

    #[test]
    fn appending_the_same_email_twice_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");

        append_account(&path, &params("me@example.com")).unwrap();
        append_account(&path, &params("me@example.com")).unwrap();

        let parsed: crate::config::ServerConfig =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed.accounts.len(), 1);
    }

    #[test]
    fn removal_drops_only_the_matching_account() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        append_account(&path, &params("a@example.com")).unwrap();
        append_account(&path, &params("b@example.com")).unwrap();

        remove_account_table(&path, "a@example.com").unwrap();

        let parsed: crate::config::ServerConfig =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed.accounts.len(), 1);
        assert_eq!(parsed.accounts[0].email, "b@example.com");
    }

    #[test]
    fn removing_an_unknown_email_is_not_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        append_account(&path, &params("a@example.com")).unwrap();

        remove_account_table(&path, "ghost@example.com").unwrap();

        let parsed: crate::config::ServerConfig =
            toml::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(parsed.accounts.len(), 1);
    }
}
