//! Configuration management for mail-cat server.

use serde::{Deserialize, Serialize};
use std::path::Path;

/// Server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// gRPC server address and port.
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String,

    /// Path to sled database directory.
    #[serde(default = "default_db_path")]
    pub db_path: String,

    /// Path to Tantivy search index directory.
    #[serde(default = "default_index_path")]
    pub index_path: String,

    /// Email accounts configuration.
    #[serde(default)]
    pub accounts: Vec<AccountConfig>,

    /// Path to the mail-cat-ui binary, launched when the user clicks
    /// "Apri Mail Cat" in a desktop notification.
    /// Defaults to `~/.local/bin/mail-cat-ui` (the `just install` location).
    #[serde(default = "default_ui_bin")]
    pub ui_bin: String,
}

/// SMTP security mode in the config file.
///
/// ```toml
/// [accounts.smtp]
/// host     = "mail.example.com"
/// port     = 587
/// security = "start_tls"   # or "smtps"
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SmtpSecurityConfig {
    Smtps,
    #[default]
    StartTls,
}

/// Optional SMTP sub-section inside an `[[accounts]]` block.
///
/// `username` and `password` are optional; when absent the account-level
/// `username`/`password` are used as SMTP credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmtpAccountConfig {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub security: SmtpSecurityConfig,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub name: String,
    pub email: String,
    pub provider_type: ProviderTypeConfig,
    // ── IMAP ─────────────────────────────────────────────────────────────────
    pub imap_host: String,
    pub imap_port: u16,
    pub use_tls: bool,
    pub username: String,
    /// Plaintext password. Present only transiently: on startup the daemon
    /// moves it into the OS keychain and rewrites the file without it (see
    /// `secret_ingest`). Absent (`None`) means "already in the keychain".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    // ── SMTP ─────────────────────────────────────────────────────────────────
    /// Dedicated SMTP configuration. Without this block the account cannot
    /// send messages.
    #[serde(default)]
    pub smtp: Option<SmtpAccountConfig>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderTypeConfig {
    Imap,
    GmailApi,
    OutlookApi,
}

impl From<ProviderTypeConfig> for mc_domain::ProviderType {
    fn from(config: ProviderTypeConfig) -> Self {
        match config {
            ProviderTypeConfig::Imap => Self::Imap,
            ProviderTypeConfig::GmailApi => Self::GmailApi,
            ProviderTypeConfig::OutlookApi => Self::OutlookApi,
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            grpc_addr: default_grpc_addr(),
            db_path: default_db_path(),
            index_path: default_index_path(),
            accounts: vec![],
            ui_bin: default_ui_bin(),
        }
    }
}

fn default_grpc_addr() -> String {
    "127.0.0.1:50051".to_string()
}

fn default_db_path() -> String {
    xdg_data_dir("db")
}

fn default_index_path() -> String {
    xdg_data_dir("index")
}

fn default_ui_bin() -> String {
    std::env::var("HOME")
        .map(|h| format!("{h}/.local/bin/mail-cat-ui"))
        .unwrap_or_else(|_| "mail-cat-ui".to_string())
}

/// Returns `~/.local/share/mail-cat/<subdir>`, falling back to `./data/<subdir>`.
fn xdg_data_dir(subdir: &str) -> String {
    std::env::var("HOME")
        .map(|h| format!("{h}/.local/share/mail-cat/{subdir}"))
        .unwrap_or_else(|_| format!("./data/{subdir}"))
}

impl ServerConfig {
    /// Load configuration from TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config = toml::from_str(&contents)?;
        Ok(config)
    }

    /// Returns the default config file path: `$MAIL_CAT_CONFIG` →
    /// `~/.config/mail-cat/config.toml` → `./config.toml`.
    pub fn default_path() -> String {
        std::env::var("MAIL_CAT_CONFIG").unwrap_or_else(|_| {
            std::env::var("HOME")
                .map(|h| format!("{h}/.config/mail-cat/config.toml"))
                .unwrap_or_else(|_| "./config.toml".to_string())
        })
    }

    /// Load from file, logging clearly if the file is missing or unparseable.
    /// Returns default config (no accounts) only as a last resort.
    pub fn load_or_default(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        match Self::from_file(path) {
            Ok(cfg) => cfg,
            Err(e) => {
                if path.exists() {
                    // File exists but failed to parse — this is almost certainly a bug.
                    eprintln!(
                        "[mail-cat] ERROR: config file '{}' exists but could not be parsed: {e}\n\
                         Falling back to empty configuration (no accounts will be synced).",
                        path.display()
                    );
                } else {
                    eprintln!(
                        "[mail-cat] WARN: config file '{}' not found.\n\
                         Create it to configure email accounts. \
                         Falling back to empty configuration.",
                        path.display()
                    );
                }
                Self::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.grpc_addr, "127.0.0.1:50051");
        assert!(config.db_path.ends_with("mail-cat/db") || config.db_path.ends_with("data/db"));
    }

    #[test]
    fn parse_smtp_sub_table() {
        let toml = r#"
[[accounts]]
name = "Test"
email = "test@example.com"
provider_type = "imap"
imap_host = "imap.example.com"
imap_port = 993
use_tls = true
username = "test@example.com"
password = "secret"

[accounts.smtp]
host = "smtp.example.com"
port = 587
security = "start_tls"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.accounts.len(), 1);
        let smtp = config.accounts[0].smtp.as_ref().expect("smtp should be Some");
        assert_eq!(smtp.host, "smtp.example.com");
        assert_eq!(smtp.port, 587);
        assert!(matches!(smtp.security, SmtpSecurityConfig::StartTls));
    }

    #[test]
    fn serialize_deserialize() {
        let config = ServerConfig {
            grpc_addr: "0.0.0.0:9000".into(),
            db_path: "/var/lib/mail-cat".into(),
            index_path: "/var/lib/mail-cat/index".into(),
            accounts: vec![AccountConfig {
                name: "Test Account".into(),
                email: "test@example.com".into(),
                provider_type: ProviderTypeConfig::Imap,
                imap_host: "imap.example.com".into(),
                imap_port: 993,
                use_tls: true,
                username: "test@example.com".into(),
                password: Some("secret".into()),
                smtp: Some(SmtpAccountConfig {
                    host: "smtp.example.com".into(),
                    port: 587,
                    security: SmtpSecurityConfig::StartTls,
                    username: None,
                    password: None,
                }),
            }],
            ui_bin: "/usr/local/bin/mail-cat-ui".into(),
        };

        let toml = toml::to_string_pretty(&config).unwrap();
        let parsed: ServerConfig = toml::from_str(&toml).unwrap();

        assert_eq!(parsed.grpc_addr, config.grpc_addr);
        assert_eq!(parsed.accounts.len(), 1);
        assert_eq!(parsed.accounts[0].name, "Test Account");
    }
}
