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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub name: String,
    pub email: String,
    pub provider_type: ProviderTypeConfig,
    pub imap_host: String,
    pub imap_port: u16,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub use_tls: bool,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderTypeConfig {
    Imap,
    GmailApi,
    OutlookApi,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            grpc_addr: default_grpc_addr(),
            db_path: default_db_path(),
            index_path: default_index_path(),
            accounts: vec![],
        }
    }
}

fn default_grpc_addr() -> String {
    "127.0.0.1:50051".to_string()
}

fn default_db_path() -> String {
    "./data/db".to_string()
}

fn default_index_path() -> String {
    "./data/index".to_string()
}

impl ServerConfig {
    /// Load configuration from TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let config = toml::from_str(&contents)?;
        Ok(config)
    }

    /// Save configuration to TOML file.
    pub fn to_file(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let contents = toml::to_string_pretty(self)?;
        std::fs::write(path, contents)?;
        Ok(())
    }

    /// Load from file if exists, otherwise return default.
    pub fn load_or_default(path: impl AsRef<Path>) -> Self {
        Self::from_file(path).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.grpc_addr, "127.0.0.1:50051");
        assert_eq!(config.db_path, "./data/db");
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
                smtp_host: "smtp.example.com".into(),
                smtp_port: 587,
                use_tls: true,
                username: "test@example.com".into(),
                password: "secret".into(),
            }],
        };

        let toml = toml::to_string_pretty(&config).unwrap();
        let parsed: ServerConfig = toml::from_str(&toml).unwrap();

        assert_eq!(parsed.grpc_addr, config.grpc_addr);
        assert_eq!(parsed.accounts.len(), 1);
        assert_eq!(parsed.accounts[0].name, "Test Account");
    }
}
