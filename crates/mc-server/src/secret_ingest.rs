//! Live migration of plaintext secrets from `config.toml` into the OS keychain.
//!
//! The user drops `password = "..."` into the config file; the daemon notices,
//! moves it into the keychain (keyed by account email), and rewrites the file
//! without it, leaving a breadcrumb comment. The operation is idempotent — it
//! only acts on plaintext, so our own rewrite triggers no further work and the
//! watcher can never loop.

use std::path::Path;

use anyhow::{Context, Result};
use mc_domain::SecretStore;
use toml_edit::DocumentMut;

const SERVICE: &str = "mail-cat";

/// Remove any plaintext passwords from the config file, storing them in the
/// keychain. Idempotent; returns `true` if the file was changed.
pub fn ingest_plaintext_secrets(config_path: &Path, store: &dyn SecretStore) -> Result<bool> {
    let contents = match std::fs::read_to_string(config_path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e).context("reading config for secret ingest"),
    };

    // ponytail: on partial write (editor truncate+write) the parse fails; we
    // skip and the next watcher event (complete file) picks it up. No debouncer.
    let mut doc: DocumentMut = contents.parse().context("parsing config as TOML")?;

    let Some(accounts) = doc
        .get_mut("accounts")
        .and_then(|i| i.as_array_of_tables_mut())
    else {
        return Ok(false);
    };

    let mut changed = false;
    for table in accounts.iter_mut() {
        let Some(email) = table
            .get("email")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
        else {
            continue; // malformed account block; leave it alone
        };

        let imap_pw = take_str(table, "password");
        let smtp_pw = table
            .get_mut("smtp")
            .and_then(|i| i.as_table_mut())
            .and_then(|smtp| take_str(smtp, "password"));

        if imap_pw.is_none() && smtp_pw.is_none() {
            continue; // nothing plaintext to migrate
        }

        // Merge into any existing bundle so OAuth/app-password secrets survive.
        let mut secrets = store.load(&email)?.unwrap_or_default();
        if imap_pw.is_some() {
            secrets.password = imap_pw;
        }
        if smtp_pw.is_some() {
            secrets.smtp_password = smtp_pw;
        }
        store
            .store(&email, &secrets)
            .with_context(|| format!("storing secrets for {email} in keychain"))?;

        // Breadcrumb above this account's block.
        let note = format!("# password → OS keychain (service \"{SERVICE}\", key \"{email}\")\n");
        let decor = table.decor_mut();
        let prefix = decor
            .prefix()
            .and_then(|r| r.as_str())
            .unwrap_or("")
            .to_owned();
        if !prefix.contains(note.trim()) {
            decor.set_prefix(format!("{prefix}{note}"));
        }
        changed = true;
    }

    if changed {
        // Back up the original once, then rewrite in place.
        let bak = config_path.with_extension("toml.bak");
        if !bak.exists() {
            let _ = std::fs::copy(config_path, &bak);
        }
        std::fs::write(config_path, doc.to_string()).context("writing migrated config")?;
    }
    Ok(changed)
}

/// Take key `k` from `table` if it holds a non-empty string.
fn take_str(table: &mut toml_edit::Table, k: &str) -> Option<String> {
    let val = table.get(k)?.as_str()?.to_owned();
    if val.is_empty() {
        return None;
    }
    table.remove(k);
    Some(val)
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

    const CONFIG: &str = r#"grpc_addr = "127.0.0.1:50051"

[[accounts]]
name = "Test"
email = "test@example.com"
provider_type = "imap"
imap_host = "imap.example.com"
imap_port = 993
use_tls = true
username = "test@example.com"
password = "hunter2"

[accounts.smtp]
host = "smtp.example.com"
port = 587
password = "smtp-secret"
"#;

    #[test]
    fn migrates_then_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, CONFIG).unwrap();
        let store = MockStore::default();

        // First pass migrates both secrets and rewrites the file.
        assert!(ingest_plaintext_secrets(&path, &store).unwrap());
        let saved = store.load("test@example.com").unwrap().unwrap();
        assert_eq!(saved.password.as_deref(), Some("hunter2"));
        assert_eq!(saved.smtp_password.as_deref(), Some("smtp-secret"));

        let rewritten = std::fs::read_to_string(&path).unwrap();
        assert!(
            !rewritten.contains("hunter2"),
            "plaintext must be gone:\n{rewritten}"
        );
        assert!(!rewritten.contains("smtp-secret"));
        assert!(rewritten.contains("# password → OS keychain"));
        assert!(rewritten.contains("imap_host"), "non-secret keys preserved");
        assert!(path.with_extension("toml.bak").exists(), "backup written");

        // Second pass: no plaintext left → no-op, file unchanged.
        assert!(!ingest_plaintext_secrets(&path, &store).unwrap());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), rewritten);
    }
}
